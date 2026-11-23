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
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// maxUDPDatagramSize is the largest possible UDP payload (65535 - 20 byte IP
// header - 8 byte UDP header). Every datagram fits into one receive buffer,
// so a single recvmsg always drains a whole datagram and message boundaries
// are preserved.
const maxUDPDatagramSize = 65507

// maxDatagramQueue bounds how many undelivered datagrams may be buffered
// per connection. When the limit is reached the poller stops draining for
// the moment; epoll in level-triggered mode fires again once the queue
// makes room.
const maxDatagramQueue = 1024

// UDPConn is a message-based connection on top of a UDP socket.
//
// Unlike the stream Connection, every ReadFrom consumes exactly one whole
// datagram. A datagram that does not fit into the caller's buffer is
// truncated: the remaining bytes are discarded together with that datagram
// and ErrShortDatagram is returned, instead of leaking into the next read.
type UDPConn interface {
	// net.Conn provides Read/Write (connected use), Close, LocalAddr,
	// RemoteAddr and the deadline setters.
	net.Conn
	// ReadFrom/WriteTo give packet-oriented, per-datagram access.
	net.PacketConn

	// Fd returns the underlying socket file descriptor.
	Fd() int

	// ReadFromUDP acts like ReadFrom but returns both the source and the
	// local destination address of the datagram.
	ReadFromUDP(b []byte) (n int, src, dst *net.UDPAddr, err error)

	// WriteToUDP acts like WriteTo but takes a *net.UDPAddr.
	WriteToUDP(b []byte, addr *net.UDPAddr) (n int, err error)
}

// datagram holds one complete received message together with the addresses
// it arrived with. The addresses are bound to this message and never shared
// with a later one.
type datagram struct {
	data      []byte
	length    int          // true datagram length as reported by the kernel
	truncated bool         // kernel could not deliver the whole datagram
	src       *net.UDPAddr // source address of this datagram
	dst       *net.UDPAddr // local destination address the datagram was sent to
}

var _ UDPConn = (*udpConn)(nil)

// udpConn implements UDPConn. The poller goroutine is the only one that
// runs recvmsg and appends to the queue; reader goroutines only pop from the
// queue behind mu.
type udpConn struct {
	fd        int
	network   string
	localAddr *net.UDPAddr
	connected bool // bound to a fixed peer (created via DialUDP)
	peerAddr  *net.UDPAddr

	operator *FDOperator

	mu      sync.Mutex
	cond    *sync.Cond
	queue   []*datagram
	closed  bool
	closing uint32 // atomic: set once Close starts

	readDeadline  atomic.Int64 // UnixNano, 0 if not set
	writeDeadline atomic.Int64 // UnixNano, 0 if not set

	// writeNotify is closed by the poller when the socket becomes writable
	// after a non-blocking send hit EAGAIN.
	writeMu     sync.Mutex
	writeNotify chan struct{}

	// done is closed once when Close runs or the poller reports a hangup.
	// Blocked writes (and any other waiter) select on it to wake up.
	done chan struct{}

	closeOnce    sync.Once // full Close (detach + close fd)
	shutdownOnce sync.Once // wake every blocked reader/writer
}

func newUDPConn(fd int, network string, local *net.UDPAddr) (*udpConn, error) {
	c := &udpConn{
		fd:        fd,
		network:   network,
		localAddr: local,
	}
	c.cond = sync.NewCond(&c.mu)
	c.done = make(chan struct{})

	poll := pollmanager.Pick()
	op := poll.Alloc()
	op.FD = fd
	op.OnRead = c.onRead
	op.OnHup = c.onHup
	op.OnWrite = c.onWritable
	c.operator = op

	if err := op.Control(PollReadable); err != nil {
		op.Free()
		return nil, err
	}
	return c, nil
}

// Fd implements UDPConn.
func (c *udpConn) Fd() int {
	return c.fd
}

// LocalAddr implements net.Conn.
func (c *udpConn) LocalAddr() net.Addr {
	return c.localAddr
}

// RemoteAddr implements net.Conn. It returns nil for an unconnected socket.
func (c *udpConn) RemoteAddr() net.Addr {
	if c.connected {
		return c.peerAddr
	}
	return nil
}

// SetDeadline implements net.Conn.
func (c *udpConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

// SetReadDeadline implements net.Conn.
func (c *udpConn) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		c.readDeadline.Store(0)
	} else {
		c.readDeadline.Store(t.UnixNano())
	}
	// wake readers so they re-evaluate the deadline
	c.mu.Lock()
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

// SetWriteDeadline implements net.Conn.
func (c *udpConn) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		c.writeDeadline.Store(0)
	} else {
		c.writeDeadline.Store(t.UnixNano())
	}
	return nil
}

// Close implements net.Conn. It is idempotent and wakes every blocked
// ReadFrom with ErrConnClosed.
func (c *udpConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		atomic.StoreUint32(&c.closing, 1)
		c.shutdown()

		// detach first so the poller stops touching the fd, then close it.
		if e := c.operator.Control(PollDetach); e != nil {
			err = e
		}
		c.operator.Free()
		if e := syscall.Close(c.fd); e != nil && err == nil {
			err = e
		}
	})
	return err
}

// shutdown marks the connection closed and wakes every blocked reader and
// writer. It is safe to call from both the user (Close) and the poller
// (onHup).
func (c *udpConn) shutdown() {
	c.shutdownOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cond.Broadcast()
		c.mu.Unlock()

		c.writeMu.Lock()
		if c.writeNotify != nil {
			close(c.writeNotify)
			c.writeNotify = nil
		}
		c.writeMu.Unlock()
		close(c.done)
	})
}

// isClosing reports whether Close has started.
func (c *udpConn) isClosing() bool {
	return atomic.LoadUint32(&c.closing) != 0
}
