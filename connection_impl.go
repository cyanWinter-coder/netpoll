// Copyright 2022 CloudWeGo Authors
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

//go:build !windows

package netpoll

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type connState = int32

const (
	connStateNone         = 0
	connStateConnected    = 1
	connStateDisconnected = 2
)

// timerState holds the generational deadline state of one direction.
//
// Every deadline assignment bumps generation and installs a fresh timerNode;
// a node that becomes due with an outdated generation is ignored by the
// timerScheduler. Read and write each keep their own state, so updating one
// direction never affects the other.
type timerState struct {
	generation uint64     // incremented on every (re)schedule; 0 means no node scheduled
	node       *timerNode // node currently registered in the scheduler, nil if cleared
}

// connection is the implementation of Connection
type connection struct {
	netFD
	onEvent
	locker
	operator      *FDOperator
	readTimeout   time.Duration
	readDeadline  int64 // UnixNano (atomic). it overwrites readTimeout. 0 if not set.
	readTrigger   chan error
	waitReadSize  int64
	writeTimeout  time.Duration
	writeDeadline int64 // UnixNano (atomic). it overwrites writeTimeout. 0 if not set.
	writeTrigger  chan error
	// timerMu protects timers and the generation counters against concurrent
	// deadline updates / clears / timeout firing / connection close.
	timerMu       sync.Mutex
	timers        [2]timerState // index by timerDir: timerRead / timerWrite
	inputBuffer   *LinkBuffer
	outputBuffer  *LinkBuffer
	outputBarrier *barrier
	maxSize       int       // The maximum size of data between two Release().
	bookSize      int       // The size of data that can be read at once.
	state         connState // Connection state should be changed sequentially.
}

var (
	_ Connection = &connection{}
	_ Reader     = &connection{}
	_ Writer     = &connection{}
)

// Reader implements Connection.
func (c *connection) Reader() Reader {
	return c
}

// Writer implements Connection.
func (c *connection) Writer() Writer {
	return c
}

// IsActive implements Connection.
func (c *connection) IsActive() bool {
	return c.isCloseBy(none)
}

// SetIdleTimeout implements Connection.
func (c *connection) SetIdleTimeout(timeout time.Duration) error {
	if timeout > 0 {
		return c.SetKeepAlive(int(timeout.Seconds()))
	}
	return nil
}

// SetReadTimeout implements Connection.
func (c *connection) SetReadTimeout(timeout time.Duration) error {
	if timeout >= 0 {
		c.readTimeout = timeout
	}
	atomic.StoreInt64(&c.readDeadline, 0)
	// a timeout change cancels any deadline scheduled for an earlier read
	c.clearReadTimer()
	return nil
}

// SetWriteTimeout implements Connection.
func (c *connection) SetWriteTimeout(timeout time.Duration) error {
	if timeout >= 0 {
		c.writeTimeout = timeout
	}
	atomic.StoreInt64(&c.writeDeadline, 0)
	// a timeout change cancels any deadline scheduled for an earlier write
	c.clearWriteTimer()
	return nil
}

// SetDeadline implements net.Conn.SetDeadline
func (c *connection) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

// SetReadDeadline implements net.Conn.SetReadDeadline
func (c *connection) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		atomic.StoreInt64(&c.readDeadline, 0)
		c.clearReadTimer()
		return nil
	}
	v := t.UnixNano()
	atomic.StoreInt64(&c.readDeadline, v)
	c.scheduleReadTimer(v)
	return nil
}

// SetWriteDeadline implements net.Conn.SetWriteDeadline
func (c *connection) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		atomic.StoreInt64(&c.writeDeadline, 0)
		c.clearWriteTimer()
		return nil
	}
	v := t.UnixNano()
	atomic.StoreInt64(&c.writeDeadline, v)
	c.scheduleWriteTimer(v)
	return nil
}

// scheduleReadTimer installs a new-generation read deadline node. Any node
// scheduled by an earlier SetReadDeadline call becomes stale and is removed,
// so an old timeout can never fire against the new deadline.
func (c *connection) scheduleReadTimer(deadline int64) *timerNode {
	return c.scheduleTimer(timerRead, deadline)
}

// scheduleWriteTimer is the write-direction counterpart of scheduleReadTimer.
func (c *connection) scheduleWriteTimer(deadline int64) *timerNode {
	return c.scheduleTimer(timerWrite, deadline)
}

// scheduleTimer bumps the generation of one direction, removes its previous
// node from the scheduler, and registers a fresh node. Read and write use
// independent timerState entries, so each direction reschedules on its own.
// It returns the installed node so a relative (Set*Timeout) waiter can later
// release only the node it itself scheduled.
func (c *connection) scheduleTimer(dir timerDir, deadline int64) *timerNode {
	globalTimerScheduler.start()
	c.timerMu.Lock()
	st := &c.timers[dir]
	old := st.node
	st.generation++
	node := globalTimerScheduler.allocNode(c, dir, deadline, st.generation)
	st.node = node
	c.timerMu.Unlock()
	// Recycle the replaced node outside the connection lock. If it already
	// fired (popped by the loop), remove returns false and the loop owns it.
	if old != nil && globalTimerScheduler.remove(old) {
		globalTimerScheduler.nodePool.Put(old)
	}
	globalTimerScheduler.schedule(node)
	return node
}

// clearReadTimer drops the pending read timer if one exists. The generation
// bump guarantees that a timer already racing with this clear will observe a
// stale generation and refuse to fire, so clear vs. timeout has one outcome.
func (c *connection) clearReadTimer() {
	c.clearTimer(timerRead)
}

// clearWriteTimer drops the pending write timer if one exists.
func (c *connection) clearWriteTimer() {
	c.clearTimer(timerWrite)
}

func (c *connection) clearTimer(dir timerDir) {
	c.timerMu.Lock()
	st := &c.timers[dir]
	node := st.node
	st.node = nil
	st.generation++ // invalidate any node currently being dispatched
	c.timerMu.Unlock()
	if node != nil && globalTimerScheduler.remove(node) {
		// only recycle when this call actually owned and removed the node;
		// a node already popped for dispatch is recycled by the scheduler loop.
		globalTimerScheduler.nodePool.Put(node)
	}
}

// clearAllTimers removes every pending timer of the connection. Called on
// close so a destroyed connection leaves no entries in the scheduler.
func (c *connection) clearAllTimers() {
	c.clearReadTimer()
	c.clearWriteTimer()
}

// releaseTimer removes node only if it is still the live node of the
// direction. A relative Set*Timeout waiter uses it on exit so that it cannot
// tear down a timer installed by a concurrent Set*Deadline call: if the live
// node differs, a newer deadline owns the slot and this call is a no-op.
func (c *connection) releaseTimer(dir timerDir, node *timerNode) {
	if node == nil {
		return
	}
	c.timerMu.Lock()
	if c.timers[dir].node != node {
		c.timerMu.Unlock()
		return
	}
	c.timers[dir].node = nil
	c.timerMu.Unlock()
	if globalTimerScheduler.remove(node) {
		globalTimerScheduler.nodePool.Put(node)
	}
}

// ------------------------------------------ implement zero-copy reader ------------------------------------------

// Next implements Connection.
func (c *connection) Next(n int) (p []byte, err error) {
	if err = c.waitRead(n); err != nil {
		return p, err
	}
	return c.inputBuffer.Next(n)
}

// Peek implements Connection.
func (c *connection) Peek(n int) (buf []byte, err error) {
	if err = c.waitRead(n); err != nil {
		return buf, err
	}
	return c.inputBuffer.Peek(n)
}

// Skip implements Connection.
func (c *connection) Skip(n int) (err error) {
	if err = c.waitRead(n); err != nil {
		return err
	}
	return c.inputBuffer.Skip(n)
}

// Release implements Connection.
func (c *connection) Release() (err error) {
	// Check inputBuffer length first to reduce contention in mux situation.
	// c.operator.do competes with c.inputs/c.inputAck
	if c.inputBuffer.Len() == 0 && c.operator.do() {
		maxSize := c.inputBuffer.calcMaxSize()
		// Set the maximum value of maxsize equal to mallocMax to prevent GC pressure.
		if maxSize > mallocMax {
			maxSize = mallocMax
		}

		if maxSize > c.maxSize {
			c.maxSize = maxSize
		}
		// Double check length to reset tail node
		if c.inputBuffer.Len() == 0 {
			c.inputBuffer.resetTail(c.maxSize)
		}
		c.operator.done()
	}
	return c.inputBuffer.Release()
}

// Slice implements Connection.
func (c *connection) Slice(n int) (r Reader, err error) {
	if err = c.waitRead(n); err != nil {
		return nil, err
	}
	return c.inputBuffer.Slice(n)
}

// Len implements Connection.
func (c *connection) Len() (length int) {
	return c.inputBuffer.Len()
}

// Until implements Connection.
func (c *connection) Until(delim byte) (line []byte, err error) {
	var n, l int
	for {
		if err = c.waitRead(n + 1); err != nil {
			// return all the data in the buffer
			line, _ = c.inputBuffer.Next(c.inputBuffer.Len())
			return
		}

		l = c.inputBuffer.Len()
		i := c.inputBuffer.indexByte(delim, n)
		if i < 0 {
			n = l // skip all exists bytes
			continue
		}
		return c.Next(i + 1)
	}
}

// ReadString implements Connection.
func (c *connection) ReadString(n int) (s string, err error) {
	if err = c.waitRead(n); err != nil {
		return s, err
	}
	return c.inputBuffer.ReadString(n)
}

// ReadBinary implements Connection.
func (c *connection) ReadBinary(n int) (p []byte, err error) {
	if err = c.waitRead(n); err != nil {
		return p, err
	}
	return c.inputBuffer.ReadBinary(n)
}

// ReadByte implements Connection.
func (c *connection) ReadByte() (b byte, err error) {
	if err = c.waitRead(1); err != nil {
		return b, err
	}
	return c.inputBuffer.ReadByte()
}

// ------------------------------------------ implement zero-copy writer ------------------------------------------

// Malloc implements Connection.
func (c *connection) Malloc(n int) (buf []byte, err error) {
	if !c.IsActive() {
		return nil, Exception(ErrConnClosed, "when malloc")
	}
	return c.outputBuffer.Malloc(n)
}

// MallocLen implements Connection.
func (c *connection) MallocLen() (length int) {
	return c.outputBuffer.MallocLen()
}

// Flush will send all malloc data to the peer,
// so must confirm that the allocated bytes have been correctly assigned.
//
// Flush first checks whether the out buffer is empty.
// If empty, it will call syscall.Write to send data directly,
// otherwise the buffer will be sent asynchronously by the epoll trigger.
func (c *connection) Flush() error {
	if !c.IsActive() {
		return Exception(ErrConnClosed, "when flush")
	}

	if !c.lock(flushing) {
		return Exception(ErrConcurrentAccess, "when flush")
	}
	defer c.unlock(flushing)

	c.outputBuffer.Flush()
	return c.flush()
}

// MallocAck implements Connection.
func (c *connection) MallocAck(n int) (err error) {
	if !c.IsActive() {
		return Exception(ErrConnClosed, "when malloc ack")
	}
	return c.outputBuffer.MallocAck(n)
}

// Append implements Connection.
func (c *connection) Append(w Writer) (err error) {
	if !c.IsActive() {
		return Exception(ErrConnClosed, "when append")
	}
	return c.outputBuffer.Append(w)
}

// WriteString implements Connection.
func (c *connection) WriteString(s string) (n int, err error) {
	if !c.IsActive() {
		return 0, Exception(ErrConnClosed, "when write string")
	}
	return c.outputBuffer.WriteString(s)
}

// WriteBinary implements Connection.
func (c *connection) WriteBinary(b []byte) (n int, err error) {
	if !c.IsActive() {
		return 0, Exception(ErrConnClosed, "when write binary")
	}
	return c.outputBuffer.WriteBinary(b)
}

// WriteDirect implements Connection.
func (c *connection) WriteDirect(p []byte, remainCap int) (err error) {
	if !c.IsActive() {
		return Exception(ErrConnClosed, "when write direct")
	}
	return c.outputBuffer.WriteDirect(p, remainCap)
}

// WriteByte implements Connection.
func (c *connection) WriteByte(b byte) (err error) {
	if !c.IsActive() {
		return Exception(ErrConnClosed, "when write byte")
	}
	return c.outputBuffer.WriteByte(b)
}

// ------------------------------------------ implement net.Conn ------------------------------------------

// Read behavior is the same as net.Conn, it will return io.EOF if buffer is empty.
func (c *connection) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err = c.waitRead(1); err != nil {
		return 0, err
	}
	return c.inputBuffer.readCopy(p), nil
}

// Write will Flush soon.
func (c *connection) Write(p []byte) (n int, err error) {
	if !c.IsActive() {
		return 0, Exception(ErrConnClosed, "when write")
	}

	if !c.lock(flushing) {
		return 0, Exception(ErrConcurrentAccess, "when write")
	}
	defer c.unlock(flushing)

	dst, _ := c.outputBuffer.Malloc(len(p))
	n = copy(dst, p)
	c.outputBuffer.Flush()
	err = c.flush()
	return n, err
}

// Close implements Connection.
func (c *connection) Close() error {
	return c.onClose()
}

// Detach detaches the connection from poller but doesn't close it.
func (c *connection) Detach() error {
	c.detaching = true
	return c.onClose()
}

// ------------------------------------------ private ------------------------------------------

var barrierPool = sync.Pool{
	New: func() interface{} {
		return &barrier{
			bs:  make([][]byte, barriercap),
			ivs: make([]syscall.Iovec, barriercap),
		}
	},
}

// init initializes the connection with options
func (c *connection) init(conn Conn, opts *options) (err error) {
	// init buffer, barrier, finalizer
	c.readTrigger = make(chan error, 1)
	c.writeTrigger = make(chan error, 1)
	c.bookSize, c.maxSize = defaultLinkBufferSize, defaultLinkBufferSize
	c.inputBuffer, c.outputBuffer = NewLinkBuffer(defaultLinkBufferSize), NewLinkBuffer()
	c.outputBarrier = barrierPool.Get().(*barrier)
	c.state = connStateNone

	c.initNetFD(conn) // conn must be *netFD{}
	c.initFDOperator()
	c.initFinalizer()

	syscall.SetNonblock(c.fd, true)
	// enable TCP_NODELAY by default
	switch c.network {
	case "tcp", "tcp4", "tcp6":
		setTCPNoDelay(c.fd, true)
	}

	// connection initialized and prepare options
	return c.onPrepare(opts)
}

func (c *connection) initNetFD(conn Conn) {
	if nfd, ok := conn.(*netFD); ok {
		c.netFD = *nfd
		return
	}
	c.netFD = netFD{
		fd:         conn.Fd(),
		localAddr:  conn.LocalAddr(),
		remoteAddr: conn.RemoteAddr(),
	}
}

func (c *connection) initFDOperator() {
	poll := pollmanager.Pick()
	op := poll.Alloc()
	op.FD = c.fd
	op.OnRead, op.OnWrite, op.OnHup = nil, nil, c.onHup
	op.Inputs, op.InputAck = c.inputs, c.inputAck
	op.Outputs, op.OutputAck = c.outputs, c.outputAck
	c.operator = op
}

func (c *connection) initFinalizer() {
	c.AddCloseCallback(func(connection Connection) (err error) {
		// remove every pending timer first so the destroyed connection leaves
		// no entries behind and no stale timeout can touch it afterwards.
		c.clearAllTimers()
		c.stop(flushing)
		c.operator.Free()
		if err = c.netFD.Close(); err != nil {
			logger.Printf("NETPOLL: netFD close failed: %v", err)
		}
		c.closeBuffer()
		return nil
	})
}

func (c *connection) triggerRead(err error) {
	select {
	case c.readTrigger <- err:
	default:
	}
}

func (c *connection) triggerWrite(err error) {
	select {
	case c.writeTrigger <- err:
	default:
	}
}

// waitRead will wait full n bytes.
func (c *connection) waitRead(n int) (err error) {
	if n <= c.inputBuffer.Len() {
		return nil
	}
	atomic.StoreInt64(&c.waitReadSize, int64(n))
	defer atomic.StoreInt64(&c.waitReadSize, 0)
	if dl := atomic.LoadInt64(&c.readDeadline); dl > 0 {
		timeout := time.Duration(dl - time.Now().UnixNano())
		if timeout <= 0 {
			return Exception(ErrReadTimeout, c.remoteAddr.String())
		}
		// The absolute deadline timer is already installed by
		// SetReadDeadline (and kept current across updates), just wait on it.
		return c.waitReadWithTimeout(n, 0)
	} else if c.readTimeout > 0 {
		return c.waitReadWithTimeout(n, c.readTimeout)
	}
	// wait full n
	for c.inputBuffer.Len() < n {
		switch c.status(closing) {
		case poller:
			return Exception(ErrEOF, "wait read")
		case user:
			return Exception(ErrConnClosed, "wait read")
		default:
			err = <-c.readTrigger
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// waitReadWithTimeout waits full n bytes or until timeout.
//
// timeout > 0 schedules a fresh relative timer for this wait (SetReadTimeout
// mode); timeout == 0 means the absolute readDeadline timer installed by
// SetReadDeadline is already in place and is reused. Either way the wait binds
// to the timer's generation, so if the deadline is updated or cleared while
// this goroutine is blocked, only the newest generation can wake it.
func (c *connection) waitReadWithTimeout(n int, timeout time.Duration) (err error) {
	var relativeNode *timerNode
	if timeout > 0 {
		relativeNode = c.scheduleReadTimer(time.Now().Add(timeout).UnixNano())
		defer c.releaseTimer(timerRead, relativeNode)
	}

	for c.inputBuffer.Len() < n {
		switch c.status(closing) {
		case poller:
			return Exception(ErrEOF, "wait read")
		case user:
			return Exception(ErrConnClosed, "wait read")
		default:
			err = <-c.readTrigger
			if err != nil {
				// A timeout error is only honored if it was produced by the
				// current generation. Stale timeouts from superseded deadlines
				// are dropped and the wait continues on the new deadline.
				if errors.Is(err, ErrReadTimeout) && !c.isCurrentTimer(timerRead) {
					continue
				}
				return err
			}
			continue
		}
	}
	return nil
}

// isCurrentTimer reports whether the readTrigger/writeTrigger error currently
// waking a waiter was emitted by the timer generation still in effect.
func (c *connection) isCurrentTimer(dir timerDir) bool {
	c.timerMu.Lock()
	st := &c.timers[dir]
	ok := st.node != nil && st.node.generation == st.generation
	c.timerMu.Unlock()
	return ok
}

// flush writes data directly.
func (c *connection) flush() error {
	if c.outputBuffer.IsEmpty() {
		return nil
	}
	bs := c.outputBuffer.GetBytes(c.outputBarrier.bs)
	n, err := sendmsg(c.fd, bs, c.outputBarrier.ivs, false)
	if err != nil && err != syscall.EAGAIN {
		return Exception(err, "when flush")
	}
	if n > 0 {
		err = c.outputBuffer.Skip(n)
		c.outputBuffer.Release()
		if err != nil {
			return Exception(err, "when flush")
		}
	}
	// return if write all buffer.
	if c.outputBuffer.IsEmpty() {
		return nil
	}
	err = c.operator.Control(PollR2RW)
	if err != nil {
		return Exception(err, "when flush")
	}

	return c.waitFlush()
}

func (c *connection) waitFlush() (err error) {
	var relativeNode *timerNode
	if dl := atomic.LoadInt64(&c.writeDeadline); dl > 0 {
		// The absolute deadline timer is maintained by SetWriteDeadline and
		// survives deadline updates; just check it has not already passed.
		if dl-time.Now().UnixNano() <= 0 {
			return Exception(ErrWriteTimeout, c.remoteAddr.String())
		}
	} else if c.writeTimeout > 0 {
		// relative SetWriteTimeout timer, scoped to this single flush
		relativeNode = c.scheduleWriteTimer(time.Now().Add(c.writeTimeout).UnixNano())
		defer c.releaseTimer(timerWrite, relativeNode)
	} else {
		// no deadline and no timeout: block until the buffer is flushed
		return <-c.writeTrigger
	}

	// Loop because a stale timeout (the deadline was updated/cleared before
	// it fired) lands here as an ErrWriteTimeout but must not fail the flush:
	// discard it and keep waiting for the real flush completion.
	for {
		err = <-c.writeTrigger
		if errors.Is(err, ErrWriteTimeout) {
			if c.isCurrentTimer(timerWrite) {
				return err // live deadline genuinely expired
			}
			// superseded generation: the old event already removed write
			// polling; re-arm it so the still-pending buffer can drain.
			_ = c.operator.Control(PollR2RW)
			continue
		}
		return err
	}
}

func (c *connection) getState() connState {
	return atomic.LoadInt32(&c.state)
}

func (c *connection) setState(newState connState) {
	atomic.StoreInt32(&c.state, newState)
}

func (c *connection) changeState(from, to connState) bool {
	return atomic.CompareAndSwapInt32(&c.state, from, to)
}
