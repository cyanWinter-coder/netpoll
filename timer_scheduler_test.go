// Copyright 2024 CloudWeGo Authors
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
	"net"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// newFakeConn builds a minimal connection stand-in for scheduler unit tests.
// Only the fields touched by timerScheduler.fire are populated.
func newFakeConn() *connection {
	c := new(connection)
	c.readTrigger = make(chan error, 16)
	c.writeTrigger = make(chan error, 16)
	return c
}

// newTCPListener returns a plain stdlib TCP listener for connection-level tests.
func newTCPListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	MustNil(t, err)
	t.Cleanup(func() { ln.Close() })
	return ln
}

// syscallSetSmallSockBuf shrinks both socket buffers so a large write blocks.
func syscallSetSmallSockBuf(fd int) error {
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, 512); err != nil {
		return err
	}
	return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, 512)
}

// TestTimerSchedulerGenerationIgnoreOldEvent verifies requirement 1: every
// deadline assignment creates a new generation, and an event from an old
// generation that becomes due is ignored rather than acting on the new deadline.
func TestTimerSchedulerGenerationIgnoreOldEvent(t *testing.T) {
	s := globalTimerScheduler
	s.start()
	c := newFakeConn()

	// old generation: deadline 20ms away
	old := s.allocNode(c, timerRead, time.Now().Add(20*time.Millisecond).UnixNano(), 1)
	c.timers[timerRead] = timerState{generation: 1, node: old}
	s.schedule(old)

	// deadline extended before the old event fires: the old node is detached
	// from the connection (stale) but stays in the heap until its deadline,
	// and a new generation is installed.
	time.Sleep(5 * time.Millisecond)
	c.timerMu.Lock()
	c.timers[timerRead].node = nil
	c.timers[timerRead].generation = 2
	next := s.allocNode(c, timerRead, time.Now().Add(200*time.Millisecond).UnixNano(), 2)
	c.timers[timerRead].node = next
	c.timerMu.Unlock()
	s.schedule(next)
	t.Cleanup(c.clearAllTimers)

	// wait well past the original (stale) deadline: the loop will pop and
	// dispatch `old`, but fire() must reject it because generation 1 no longer
	// matches the current generation 2.
	time.Sleep(80 * time.Millisecond)
	select {
	case err := <-c.readTrigger:
		t.Fatalf("stale generation fired unexpectedly: %v", err)
	default:
	}
}

// TestTimerSchedulerCurrentGenerationFires confirms the live generation does
// fire exactly once when its deadline passes.
func TestTimerSchedulerCurrentGenerationFires(t *testing.T) {
	s := globalTimerScheduler
	s.start()
	c := newFakeConn()

	node := s.allocNode(c, timerRead, time.Now().Add(30*time.Millisecond).UnixNano(), 1)
	c.timers[timerRead] = timerState{generation: 1, node: node}
	s.schedule(node)

	select {
	case err := <-c.readTrigger:
		if !errors.Is(err, ErrReadTimeout) {
			t.Fatalf("expect ErrReadTimeout, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("current generation timer never fired")
	}
}

// TestTimerSchedulerReadWriteSeparation verifies requirement 2: updating one
// direction must leave the other direction's generation untouched.
func TestTimerSchedulerReadWriteSeparation(t *testing.T) {
	c := newFakeConn()
	t.Cleanup(c.clearAllTimers)

	// install both directions
	MustNil(t, c.SetReadDeadline(time.Now().Add(time.Hour)))
	MustNil(t, c.SetWriteDeadline(time.Now().Add(time.Hour)))

	c.timerMu.Lock()
	readGen := c.timers[timerRead].generation
	writeGen := c.timers[timerWrite].generation
	readNode := c.timers[timerRead].node
	writeNode := c.timers[timerWrite].node
	c.timerMu.Unlock()
	if readGen == 0 || writeGen == 0 {
		t.Fatal("both timers should be scheduled")
	}

	// repeatedly extend only the read deadline
	for i := 0; i < 5; i++ {
		MustNil(t, c.SetReadDeadline(time.Now().Add(time.Hour)))
	}
	c.timerMu.Lock()
	newReadGen := c.timers[timerRead].generation
	newReadNode := c.timers[timerRead].node
	keepWriteGen := c.timers[timerWrite].generation
	keepWriteNode := c.timers[timerWrite].node
	c.timerMu.Unlock()

	if newReadGen != readGen+5 {
		t.Fatalf("read generation should advance by 5, got %d want %d", newReadGen, readGen+5)
	}
	if newReadNode == readNode {
		t.Fatal("read node should be replaced on update")
	}
	if keepWriteGen != writeGen || keepWriteNode != writeNode {
		t.Fatal("write timer must not be affected by read deadline updates")
	}

	// extending write must not touch read either
	MustNil(t, c.SetWriteDeadline(time.Now().Add(2*time.Hour)))
	c.timerMu.Lock()
	readAgain := c.timers[timerRead].generation
	writeAgain := c.timers[timerWrite].generation
	c.timerMu.Unlock()
	if readAgain != newReadGen {
		t.Fatal("read generation changed on write deadline update")
	}
	if writeAgain != keepWriteGen+1 {
		t.Fatalf("write generation should advance once, got %d want %d", writeAgain, keepWriteGen+1)
	}
}

// TestTimerSchedulerHighFrequencyUpdates is the high-frequency update test.
// A deadline is extended thousands of times; the read must not time out until
// updates stop, proving old events never win against newer generations.
func TestTimerSchedulerHighFrequencyUpdates(t *testing.T) {
	globalTimerScheduler.start()
	c := newFakeConn()
	t.Cleanup(c.clearAllTimers)

	const updates = 2000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < updates; i++ {
			c.scheduleReadTimer(time.Now().Add(50 * time.Millisecond).UnixNano())
			if i%100 == 0 {
				runtime.Gosched()
			}
		}
	}()

	// while updates keep flowing, no timeout may arrive
	select {
	case err := <-c.readTrigger:
		t.Fatalf("timeout fired during deadline extensions: %v", err)
	case <-done:
	}

	c.timerMu.Lock()
	gen := c.timers[timerRead].generation
	c.timerMu.Unlock()
	if gen != updates {
		t.Fatalf("generation mismatch: got %d expected %d", gen, updates)
	}

	// after updates stop, the final deadline must still fire exactly once
	select {
	case err := <-c.readTrigger:
		if !errors.Is(err, ErrReadTimeout) {
			t.Fatalf("expect ErrReadTimeout after updates stop, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("final deadline never fired after high-frequency updates")
	}

	// no duplicate / stale timeouts afterwards
	time.Sleep(40 * time.Millisecond)
	select {
	case err := <-c.readTrigger:
		t.Fatalf("extra stale timeout delivered: %v", err)
	default:
	}
}

// TestTimerSchedulerHeapHasNoStaleEntriesAfterUpdate checks the heap does not
// accumulate dead nodes when a deadline is updated in a tight loop.
func TestTimerSchedulerHeapHasNoStaleEntriesAfterUpdate(t *testing.T) {
	s := globalTimerScheduler
	s.start()
	c := newFakeConn()

	before := s.len()
	for i := 0; i < 500; i++ {
		c.scheduleReadTimer(time.Now().Add(time.Hour).UnixNano())
	}
	if got := s.len() - before; got != 1 {
		t.Fatalf("heap should hold only the one live node, got %d entries from this conn", got)
	}
	c.clearAllTimers()
	if got := s.len() - before; got != 0 {
		t.Fatalf("heap should be empty after clear, got %d leftover entries", got)
	}
}

// TestTimerSchedulerClearVersusFireRace verifies requirement 4: clearing a
// deadline and the timeout firing concurrently settle on exactly one result,
// never a timeout delivered after the clear (which would wrongly fail an IO).
func TestTimerSchedulerClearVersusFireRace(t *testing.T) {
	globalTimerScheduler.start()
	c := newFakeConn()

	const rounds = 200
	for r := 0; r < rounds; r++ {
		c.scheduleReadTimer(time.Now().Add(time.Millisecond).UnixNano())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { // racing clear
			defer wg.Done()
			c.clearReadTimer()
		}()
		wg.Wait()

		// after clear, the node must be gone and no live generation remain
		c.timerMu.Lock()
		node := c.timers[timerRead].node
		gen := c.timers[timerRead].generation
		c.timerMu.Unlock()
		if node != nil {
			t.Fatal("clear did not remove the timer node")
		}

		// let any in-flight natural fire land, then a delivered timeout must
		// correspond to a stale (already-cleared) generation and be rejected
		time.Sleep(3 * time.Millisecond)
		select {
		case err := <-c.readTrigger:
			if errors.Is(err, ErrReadTimeout) && gen > 0 && c.isCurrentTimer(timerRead) {
				t.Fatal("timeout won the race against clear despite being cleared")
			}
		default:
		}
	}
}

// TestTimerSchedulerCloseRemovesAllTimers verifies requirement 5: closing a
// connection removes every scheduled timer, leaving no node behind and
// delivering no late timeouts.
func TestTimerSchedulerCloseRemovesAllTimers(t *testing.T) {
	s := globalTimerScheduler
	s.start()
	c := newFakeConn()

	before := s.len()
	c.scheduleReadTimer(time.Now().Add(50 * time.Millisecond).UnixNano())
	c.scheduleWriteTimer(time.Now().Add(50 * time.Millisecond).UnixNano())
	if got := s.len() - before; got != 2 {
		t.Fatalf("expect 2 scheduled nodes, got %d", got)
	}

	c.clearAllTimers() // invoked by the connection close finalizer
	c.timerMu.Lock()
	readLeft := c.timers[timerRead].node
	writeLeft := c.timers[timerWrite].node
	c.timerMu.Unlock()
	if readLeft != nil || writeLeft != nil {
		t.Fatalf("close left timers behind: read=%v write=%v", readLeft, writeLeft)
	}
	if got := s.len() - before; got != 0 {
		t.Fatalf("close left %d timer entries in the scheduler", got)
	}

	time.Sleep(80 * time.Millisecond)
	select {
	case err := <-c.readTrigger:
		t.Fatalf("read timeout delivered after close: %v", err)
	default:
	}
	select {
	case err := <-c.writeTrigger:
		t.Fatalf("write timeout delivered after close: %v", err)
	default:
	}
}

// TestTimerSchedulerFairDispatch verifies requirement 3: when a large number
// of short-deadline connections expire on the same tick, they are dispatched
// in bounded batches so no single tick monopolizes the scheduler, and every
// connection still receives its timeout (no fd is starved).
func TestTimerSchedulerFairDispatch(t *testing.T) {
	s := globalTimerScheduler
	s.start()

	const connCount = timerMaxDispatchPerTick * 3
	conns := make([]*connection, connCount)
	deadline := time.Now().Add(60 * time.Millisecond).UnixNano()
	for i := range conns {
		conns[i] = newFakeConn()
		node := s.allocNode(conns[i], timerRead, deadline, 1)
		conns[i].timers[timerRead] = timerState{generation: 1, node: node}
		s.schedule(node)
	}
	t.Cleanup(func() {
		for _, c := range conns {
			c.clearAllTimers()
		}
	})

	// all connections must eventually be served despite the per-tick cap;
	// the 3x batch size forces the scheduler through multiple ticks.
	deadlineCh := time.After(3 * time.Second)
	for i, c := range conns {
		select {
		case err := <-c.readTrigger:
			if !errors.Is(err, ErrReadTimeout) {
				t.Fatalf("conn %d got unexpected error: %v", i, err)
			}
		case <-deadlineCh:
			t.Fatalf("conn %d was starved: timeout never delivered", i)
		}
	}
}

// TestTimerSchedulerEqualDeadlineAllDrained checks that many connections with
// the identical short deadline are all served (the fairness tie-break path).
func TestTimerSchedulerEqualDeadlineAllDrained(t *testing.T) {
	s := globalTimerScheduler
	s.start()

	const n = 64
	conns := make([]*connection, n)
	deadline := time.Now().Add(40 * time.Millisecond).UnixNano()
	for i := range conns {
		conns[i] = newFakeConn()
		node := s.allocNode(conns[i], timerRead, deadline, uint64(i+1))
		conns[i].timers[timerRead] = timerState{generation: uint64(i + 1), node: node}
		s.schedule(node)
	}

	for i, c := range conns {
		select {
		case <-c.readTrigger:
		case <-time.After(2 * time.Second):
			t.Fatalf("equal-deadline conn %d never fired", i)
		}
	}
}

// TestReadDeadlineExtensionDuringBlockedRead is an end-to-end check using a
// real connection: extending the read deadline while a Read is blocked must
// keep the read alive until the newest deadline, and the old short deadline
// must not surface as a premature timeout.
func TestReadDeadlineExtensionDuringBlockedRead(t *testing.T) {
	ln := newTCPListener(t)

	conn, err := DialConnection("tcp", ln.Addr().String(), time.Second)
	MustNil(t, err)
	defer conn.Close()

	readDone := make(chan error, 1)
	go func() {
		_, e := conn.Reader().Next(64)
		readDone <- e
	}()

	// give the reader time to block
	time.Sleep(20 * time.Millisecond)

	// aggressively extend the deadline; an old short event must never win
	extendUntil := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(extendUntil) {
		MustNil(t, conn.SetReadDeadline(time.Now().Add(60*time.Millisecond)))
		time.Sleep(10 * time.Millisecond)
	}

	// no premature timeout while we were extending
	select {
	case err := <-readDone:
		t.Fatalf("read returned early during deadline extensions: %v", err)
	default:
	}

	// now let it time out on the final deadline
	select {
	case err := <-readDone:
		if !errors.Is(err, ErrReadTimeout) {
			t.Fatalf("expect ErrReadTimeout on final deadline, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked read never resolved after final deadline")
	}
}

// TestReadDeadlineClearDuringBlockedRead ensures clearing a deadline (zero
// time) while a read is waiting removes the timer and leaves the read blocked
// without a timeout, then a later close releases it.
func TestReadDeadlineClearDuringBlockedRead(t *testing.T) {
	ln := newTCPListener(t)

	conn, err := DialConnection("tcp", ln.Addr().String(), time.Second)
	MustNil(t, err)
	defer conn.Close()

	readDone := make(chan error, 1)
	go func() {
		_, e := conn.Reader().Next(64)
		readDone <- e
	}()

	time.Sleep(20 * time.Millisecond)
	MustNil(t, conn.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	time.Sleep(30 * time.Millisecond)
	MustNil(t, conn.SetReadDeadline(time.Time{})) // clear

	// the cleared deadline must not fire
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-readDone:
		t.Fatalf("read returned after deadline was cleared: %v", err)
	default:
	}

	MustNil(t, conn.Close())
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expected an error when connection closed during blocked read")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked read never released on close")
	}
}

// TestWriteDeadlineExtensionDuringBlockedFlush is the write-direction
// end-to-end check: a write deadline extended while a flush is blocked must
// keep the flush alive, and a stale short-deadline event must not surface as
// a premature write timeout. When extensions stop the final deadline wins.
func TestWriteDeadlineExtensionDuringBlockedFlush(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	MustNil(t, err)
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		ac, _ := ln.Accept()
		accepted <- ac
	}()

	conn, err := DialConnection("tcp", ln.Addr().String(), time.Second)
	MustNil(t, err)
	defer conn.Close()
	peer := <-accepted
	defer peer.Close()

	// shrink both socket buffers so a large write blocks in waitFlush
	fd := connConn(conn)
	if fd == nil {
		t.Fatalf("unexpected connection type %T", conn)
	}
	MustNil(t, syscallSetSmallSockBuf(fd.fd))

	// fill the pipe: peer never reads, so the output buffer cannot drain
	payload := make([]byte, 1<<20)
	_, err = conn.Writer().Malloc(len(payload))
	MustNil(t, err)
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- conn.Writer().Flush()
	}()

	// allow the flush to enter the blocked wait
	time.Sleep(30 * time.Millisecond)

	// keep extending the write deadline; no stale timeout may win
	extendUntil := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(extendUntil) {
		MustNil(t, conn.SetWriteDeadline(time.Now().Add(60*time.Millisecond)))
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-flushDone:
		t.Fatalf("flush returned early during deadline extensions: %v", err)
	default:
	}

	// stop extending: the final short deadline must now time out exactly once
	MustNil(t, conn.SetWriteDeadline(time.Now().Add(80*time.Millisecond)))
	select {
	case err := <-flushDone:
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("expect ErrWriteTimeout on final deadline, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked flush never resolved after final deadline")
	}
}

// TestRelativeTimeoutSurvivesConcurrentDeadline guards the ownership boundary
// between a relative SetReadTimeout wait and a concurrent SetReadDeadline:
// when the wait ends (here, because data arrives), it must release only the
// timer it scheduled and leave a deadline installed in the meantime intact for
// subsequent reads.
func TestRelativeTimeoutSurvivesConcurrentDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	MustNil(t, err)
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		ac, _ := ln.Accept()
		accepted <- ac
	}()

	conn, err := DialConnection("tcp", ln.Addr().String(), time.Second)
	MustNil(t, err)
	defer conn.Close()
	peer := <-accepted
	defer peer.Close()

	c := connConn(conn)
	if c == nil {
		t.Fatalf("unexpected connection type %T", conn)
	}
	MustNil(t, conn.SetReadTimeout(time.Hour)) // long relative timeout

	readDone := make(chan error, 1)
	go func() {
		_, e := conn.Reader().Next(64)
		readDone <- e
	}()

	// while the relative wait is blocked, install an absolute deadline
	time.Sleep(20 * time.Millisecond)
	MustNil(t, conn.SetReadDeadline(time.Now().Add(time.Hour)))
	c.timerMu.Lock()
	deadlineNode := c.timers[timerRead].node
	c.timerMu.Unlock()
	if deadlineNode == nil {
		t.Fatal("deadline node should be installed while read is blocked")
	}

	// unblock the wait by delivering data rather than waiting for a timeout
	_, err = peer.Write(make([]byte, 64))
	MustNil(t, err)
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read after data delivery should succeed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relative read never returned after data arrived")
	}

	// the absolute deadline timer must still be registered after the wait;
	// the relative waiter must not have released a node it did not own.
	c.timerMu.Lock()
	node := c.timers[timerRead].node
	gen := c.timers[timerRead].generation
	c.timerMu.Unlock()
	if node != deadlineNode {
		t.Fatal("relative wait tore down the concurrently installed deadline")
	}
	if gen == 0 {
		t.Fatal("deadline generation missing after relative wait")
	}
	c.clearAllTimers()
}

// TestSetDeadlineUpdatesBothDirectionsIndependently makes sure SetDeadline
// schedules both read and write, and clearing only one direction keeps the
// other alive (requirement 2 at the public API surface).
func TestSetDeadlineUpdatesBothDirectionsIndependently(t *testing.T) {
	ln := newTCPListener(t)

	conn, err := DialConnection("tcp", ln.Addr().String(), time.Second)
	MustNil(t, err)
	defer conn.Close()
	c := connConn(conn)
	if c == nil {
		t.Fatalf("unexpected connection type %T", conn)
	}

	MustNil(t, conn.SetDeadline(time.Now().Add(time.Hour)))
	c.timerMu.Lock()
	hasRead := c.timers[timerRead].node != nil
	hasWrite := c.timers[timerWrite].node != nil
	writeGenBefore := c.timers[timerWrite].generation
	c.timerMu.Unlock()
	MustTrue(t, hasRead && hasWrite)

	// clear only read; write timer survives
	MustNil(t, conn.SetReadDeadline(time.Time{}))
	c.timerMu.Lock()
	readCleared := c.timers[timerRead].node == nil
	writeNodeAfter := c.timers[timerWrite].node
	writeGenAfter := c.timers[timerWrite].generation
	c.timerMu.Unlock()
	MustTrue(t, readCleared)
	if writeNodeAfter == nil || writeGenAfter != writeGenBefore {
		t.Fatal("clearing read deadline must not disturb the write timer")
	}
	c.clearAllTimers()
}

// connConn extracts the embedded *connection from either a plain *connection
// or a *TCPConnection wrapper returned by DialConnection.
func connConn(conn Connection) *connection {
	switch v := conn.(type) {
	case *connection:
		return v
	case *TCPConnection:
		return &v.connection
	default:
		return nil
	}
}

// TestDeadlineFiresOnlyOnceAcrossGenerations confirms a connection that
// updates its deadline many times receives exactly one timeout per live
// generation and no extras from superseded ones.
func TestDeadlineFiresOnlyOnceAcrossGenerations(t *testing.T) {
	ln := newTCPListener(t)

	conn, err := DialConnection("tcp", ln.Addr().String(), time.Second)
	MustNil(t, err)
	defer conn.Close()
	c := connConn(conn)
	if c == nil {
		t.Fatalf("unexpected connection type %T", conn)
	}

	for i := 0; i < 100; i++ {
		MustNil(t, c.SetReadDeadline(time.Now().Add(time.Hour)))
	}
	// final short deadline
	MustNil(t, c.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

	// the single live generation must deliver exactly one timeout
	select {
	case <-c.readTrigger:
	case <-time.After(time.Second):
		t.Fatal("expected one timeout, got none")
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case <-c.readTrigger:
		t.Fatal("more than one timeout delivered across generations")
	default:
	}
}
