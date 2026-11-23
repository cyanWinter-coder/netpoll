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
	"syscall"
	"time"
)

// Write implements net.Conn. It sends b as one datagram to the connected
// peer and fails on an unconnected socket. Use WriteTo for those instead.
func (c *udpConn) Write(b []byte) (n int, err error) {
	if !c.connected {
		return 0, &net.OpError{Op: "write", Net: c.network, Err: errMissingAddress}
	}
	return c.write(b, nil)
}

// WriteTo implements net.PacketConn. Each call sends b as a single
// datagram; a datagram is never fragmented by this layer.
func (c *udpConn) WriteTo(b []byte, addr net.Addr) (n int, err error) {
	if !c.connected && addr == nil {
		return 0, &net.OpError{Op: "write", Net: c.network, Err: errMissingAddress}
	}
	if c.connected {
		// peer is fixed by connect(); ignore any caller-supplied address
		return c.write(b, nil)
	}
	udp, ok := addr.(*net.UDPAddr)
	if !ok {
		return 0, &net.OpError{Op: "write", Net: c.network, Addr: addr, Err: &net.AddrError{Err: "invalid UDP address", Addr: addr.String()}}
	}
	return c.WriteToUDP(b, udp)
}

// WriteToUDP implements UDPConn.
func (c *udpConn) WriteToUDP(b []byte, addr *net.UDPAddr) (n int, err error) {
	if c.connected {
		return c.write(b, nil)
	}
	if addr == nil {
		return 0, &net.OpError{Op: "write", Net: c.network, Err: errMissingAddress}
	}
	sa, err := ipToSockaddr(c.family(), addr.IP, addr.Port, addr.Zone)
	if err != nil {
		return 0, &net.OpError{Op: "write", Net: c.network, Addr: addr, Err: err}
	}
	return c.write(b, sa)
}

// family returns the address family of the underlying socket.
func (c *udpConn) family() int {
	if c.localAddr != nil && c.localAddr.IP != nil && c.localAddr.IP.To4() == nil &&
		len(c.localAddr.IP) == net.IPv6len {
		return syscall.AF_INET6
	}
	return syscall.AF_INET
}

// write performs a non-blocking sendto. On EAGAIN it waits for the socket
// to become writable via the poller and retries, honoring the write deadline.
func (c *udpConn) write(b []byte, sa syscall.Sockaddr) (n int, err error) {
	if c.isClosing() {
		return 0, Exception(ErrConnClosed, "udp write")
	}
	for {
		err = syscall.Sendto(c.fd, b, 0, sa)
		if err == nil {
			return len(b), nil
		}
		if err == syscall.EINTR {
			continue
		}
		if err != syscall.EAGAIN && err != syscall.EWOULDBLOCK {
			return 0, err
		}
		if err = c.waitWritable(); err != nil {
			return 0, err
		}
	}
}

// onWritable is invoked by the poller (edge-triggered) once the socket can
// accept data again after an EAGAIN send.
func (c *udpConn) onWritable(p Poll) error {
	c.writeMu.Lock()
	ch := c.writeNotify
	c.writeMu.Unlock()
	if ch != nil {
		// back to read-only monitoring; the send retry happens in write()
		_ = c.operator.Control(PollRW2R)
		close(ch)
	}
	return nil
}

// waitWritable blocks until the socket is writable, closed, or the write
// deadline passes. Each call installs a fresh notification channel so a
// stale writable event from a previous wait can never wake a later caller.
func (c *udpConn) waitWritable() error {
	ch := make(chan struct{})
	c.writeMu.Lock()
	c.writeNotify = ch
	c.writeMu.Unlock()

	// arm edge-triggered writability; onWritable switches the operator
	// back to read-only once the socket can accept data again.
	if err := c.operator.Control(PollR2RW); err != nil {
		return err
	}

	dl := c.writeDeadline.Load()
	if dl == 0 {
		select {
		case <-ch:
			return nil
		case <-c.done:
			return Exception(ErrConnClosed, "udp write")
		}
	}

	d := time.Until(time.Unix(0, dl))
	if d <= 0 {
		c.disarmWritable(ch)
		return Exception(ErrWriteTimeout, c.localAddr.String())
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		// disarm EPOLLOUT and drop the stale channel so the leftover
		// writable event cannot spuriously wake the next write waiter
		c.disarmWritable(ch)
		return Exception(ErrWriteTimeout, c.localAddr.String())
	case <-c.done:
		return Exception(ErrConnClosed, "udp write")
	}
}

// disarmWritable removes the writable subscription and clears the notify
// channel only if it is still the one this waiter installed.
func (c *udpConn) disarmWritable(ch chan struct{}) {
	_ = c.operator.Control(PollRW2R)
	c.writeMu.Lock()
	if c.writeNotify == ch {
		c.writeNotify = nil
	}
	c.writeMu.Unlock()
}
