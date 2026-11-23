// Copyright 2026 CloudWeGo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package netpoll

import (
	"net"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// onRead is invoked by the poller when the socket is readable.
// It drains every currently available datagram, one recvmsg per datagram,
// so message boundaries are never merged. Level-triggered epoll re-fires
// if the queue limit forces us to stop early or new datagrams arrive later.
func (c *udpConn) onRead(p Poll) error {
	scratch := make([]byte, maxUDPDatagramSize)
	oob := make([]byte, unix.CmsgSpace(unix.SizeofInet4Pktinfo)+unix.CmsgSpace(unix.SizeofInet6Pktinfo))

	for {
		if c.isClosing() {
			return nil
		}
		c.mu.Lock()
		full := len(c.queue) >= maxDatagramQueue
		c.mu.Unlock()
		if full {
			return nil
		}

		// MSG_TRUNC as an input flag makes the kernel report the real
		// datagram length even when it exceeds the buffer; without it the
		// return value alone cannot distinguish a full datagram from a
		// truncated one.
		n, oobn, _, sa, err := syscall.Recvmsg(c.fd, scratch, oob, syscall.MSG_TRUNC)
		if err != nil {
			// nothing left to read; LT epoll will notify us again later
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				return nil
			}
			if err == syscall.EINTR {
				continue
			}
			// a real socket error: surface it as a hangup
			logger.Printf("NETPOLL: udp recvmsg failed: %v", err)
			c.onHup(p)
			return nil
		}

		// n is the true datagram length (MSG_TRUNC). The internal scratch
		// buffer is sized for the largest possible datagram, so it normally
		// holds the whole message; the truncation check below also covers
		// the rare case where the kernel reported more than it delivered.
		got := n
		kernelTruncated := false
		if got > len(scratch) {
			got = len(scratch)
			kernelTruncated = true
		}
		msg := &datagram{
			// copy out: scratch is reused for the next datagram
			data:      append([]byte(nil), scratch[:got]...),
			length:    n,
			truncated: kernelTruncated,
			src:       sockaddrToUDP(sa),
			dst:       parseDstAddr(oob[:oobn]),
		}

		c.mu.Lock()
		c.queue = append(c.queue, msg)
		// wake one waiting reader; it leaves the rest of the queue intact
		c.cond.Signal()
		c.mu.Unlock()
	}
}

// onHup is invoked by the poller on socket errors. User Close drives the
// real shutdown; here we only wake blocked readers.
func (c *udpConn) onHup(p Poll) error {
	if c.isClosing() {
		return nil
	}
	atomic.StoreUint32(&c.closing, 1)
	c.shutdown()
	return nil
}

// parseDstAddr extracts the local destination address of a received
// datagram from the IP_PKTINFO / IPV6_RECVPKTINFO control messages.
// It returns nil when no control message is present.
func parseDstAddr(oob []byte) *net.UDPAddr {
	if len(oob) == 0 {
		return nil
	}
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil
	}
	for i := range msgs {
		m := &msgs[i]
		switch {
		case m.Header.Level == syscall.IPPROTO_IP && m.Header.Type == unix.IP_PKTINFO:
			if len(m.Data) < unix.SizeofInet4Pktinfo {
				continue
			}
			info := (*unix.Inet4Pktinfo)(unsafe.Pointer(&m.Data[0]))
			return &net.UDPAddr{IP: net.IP(info.Addr[:]).To4()}
		case m.Header.Level == syscall.IPPROTO_IPV6 && m.Header.Type == unix.IPV6_PKTINFO:
			if len(m.Data) < unix.SizeofInet6Pktinfo {
				continue
			}
			info := (*unix.Inet6Pktinfo)(unsafe.Pointer(&m.Data[0]))
			ip := make(net.IP, net.IPv6len)
			copy(ip, info.Addr[:])
			return &net.UDPAddr{IP: ip, Zone: zoneFromIndex(int(info.Ifindex))}
		}
	}
	return nil
}

// sockaddrToUDP converts a kernel sockaddr to *net.UDPAddr.
func sockaddrToUDP(sa syscall.Sockaddr) *net.UDPAddr {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		return &net.UDPAddr{IP: append(net.IP(nil), sa.Addr[:]...), Port: sa.Port}
	case *syscall.SockaddrInet6:
		return &net.UDPAddr{
			IP:   append(net.IP(nil), sa.Addr[:]...),
			Port: sa.Port,
			Zone: zoneFromIndex(int(sa.ZoneId)),
		}
	default:
		return nil
	}
}

func zoneFromIndex(index int) string {
	if index == 0 {
		return ""
	}
	if ifi, err := net.InterfaceByIndex(index); err == nil {
		return ifi.Name
	}
	return ""
}

// ReadFrom implements net.PacketConn. It blocks until exactly one whole
// datagram is available. Concurrent calls compete for complete datagrams;
// a single datagram is never split between two callers.
func (c *udpConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	n, src, _, rerr := c.readDatagram(b)
	if rerr != nil {
		return n, nil, rerr
	}
	return n, src, nil
}

// Read implements net.Conn. It reads one datagram and discards its source
// address. It is mainly useful on connected sockets created via DialUDP.
// Datagram boundaries are preserved exactly as in ReadFrom.
func (c *udpConn) Read(b []byte) (n int, err error) {
	n, _, _, err = c.readDatagram(b)
	return n, err
}

// ReadFromUDP implements UDPConn.
func (c *udpConn) ReadFromUDP(b []byte) (n int, src, dst *net.UDPAddr, err error) {
	return c.readDatagram(b)
}

// readDatagram delivers one datagram into b. If the datagram is larger than
// b the prefix is copied, the remainder is discarded together with the
// datagram and ErrShortDatagram is returned, instead of leaking into the
// next read. The datagram's source and destination addresses travel with it.
func (c *udpConn) readDatagram(b []byte) (n int, src, dst *net.UDPAddr, err error) {
	// A zero-length buffer consumes nothing, matching net.UDPConn behavior.
	if len(b) == 0 {
		return 0, nil, nil, nil
	}
	msg, err := c.waitDatagram()
	if err != nil {
		return 0, nil, nil, err
	}
	n = copy(b, msg.data)
	src, dst = msg.src, msg.dst
	// Truncation happens when the caller's buffer cannot hold the whole
	// datagram, or when the kernel already had to drop part of it. The
	// remainder is discarded together with this datagram: the next call
	// starts fresh with the next datagram, never with the leftover bytes.
	if msg.truncated || msg.length > len(b) {
		return n, src, dst, Exception(ErrShortDatagram, "")
	}
	return n, src, dst, nil
}

// waitDatagram pops one complete datagram from the queue, blocking while
// the queue is empty. A zero-length datagram is a valid result.
func (c *udpConn) waitDatagram() (msg *datagram, err error) {
	if c.readDeadline.Load() > 0 {
		// A watchdog is needed because sync.Cond cannot select on a timer.
		// It sleeps until the deadline (or a short cap, so SetReadDeadline
		// shortening is picked up promptly), then broadcasts; the loop below
		// re-checks the actual deadline before returning a timeout.
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				d := time.Until(time.Unix(0, c.readDeadline.Load()))
				if d <= 0 {
					d = 0
				}
				if d > deadlineWatchCap {
					d = deadlineWatchCap
				}
				timer := time.NewTimer(d)
				select {
				case <-done:
					timer.Stop()
					return
				case <-timer.C:
					// deadline may have been cleared or extended; re-read it
					if c.readDeadline.Load() <= 0 {
						return
					}
					c.mu.Lock()
					c.cond.Broadcast()
					c.mu.Unlock()
				}
			}
		}()
	}

	c.mu.Lock()
	for {
		if len(c.queue) > 0 {
			msg = c.queue[0]
			c.queue = c.queue[1:]
			c.mu.Unlock()
			return msg, nil
		}
		if c.closed {
			c.mu.Unlock()
			return nil, Exception(ErrConnClosed, "udp read")
		}
		if d := c.readDeadline.Load(); d > 0 && time.Now().UnixNano() >= d {
			c.mu.Unlock()
			return nil, Exception(ErrReadTimeout, c.localAddr.String())
		}
		c.cond.Wait()
	}
}

// deadlineWatchCap bounds how long a watchdog goroutine sleeps at once so
// that shortening the read deadline via SetReadDeadline takes effect within
// this window rather than only after the original deadline.
const deadlineWatchCap = 20 * time.Millisecond
