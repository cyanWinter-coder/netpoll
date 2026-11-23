// Copyright 2025 CloudWeGo Authors
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
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Multiple Close calls must share the same completion signal and return a
// stable result, and Wait must exit in bounded time.
func TestPollCloseIdempotent(t *testing.T) {
	p, err := openDefaultPoll()
	MustNil(t, err)
	waitErr := make(chan error, 1)
	go func() { waitErr <- p.Wait() }()

	const closers = 8
	errs := make([]error, closers)
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = p.Close()
		}(i)
	}
	wg.Wait()
	// all Close callers share the same stable result
	for i := 1; i < closers; i++ {
		Assert(t, errs[i] == errs[0], errs[i], errs[0])
	}
	// bounded exit: Wait returns in time
	select {
	case err = <-waitErr:
		MustNil(t, err)
	case <-time.After(time.Second):
		t.Fatal("poll.Wait did not exit in time after Close")
	}
	// the completion signal must be closed after Wait returned
	select {
	case <-p.Done():
	default:
		t.Fatal("Done() is not closed after Wait returned")
	}
	// Close after exit: still the same result, no write to the closed wakeup fd
	Assert(t, p.Close() == errs[0])
	Assert(t, atomic.LoadInt32(&p.state) == pollStateClosed)
}

// After the poll stops, Trigger and Control must atomically reject new tasks
// with ErrClosed, and no queued wakeup may be left.
func TestPollRejectTaskAfterClose(t *testing.T) {
	p, err := openDefaultPoll()
	MustNil(t, err)
	go func() { _ = p.Wait() }()
	MustNil(t, p.Close())
	<-p.Done()

	err = p.Trigger()
	Assert(t, errors.Is(err, ErrClosed), err)

	rfd, wfd := GetSysFdPairs()
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)
	op := &FDOperator{FD: rfd}
	err = p.Control(op, PollReadable)
	Assert(t, errors.Is(err, ErrClosed), err)
	// no queued wakeup task left
	Equal(t, atomic.LoadInt32(&p.queued), int32(0))
}

// Events already queued in epoll must not run connection callbacks once the
// poll is closing; only the wakeup event is drained to finish the exit.
func TestPollQueuedEventsNotExecutedAfterClose(t *testing.T) {
	p, err := openDefaultPoll()
	MustNil(t, err)
	p.Reset(8, barriercap)

	rfd, wfd := GetSysFdPairs()
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)

	var reads int32
	rop := &FDOperator{FD: rfd, OnRead: func(p Poll) error {
		atomic.AddInt32(&reads, 1)
		return nil
	}}
	MustNil(t, p.Control(rop, PollReadable))

	_, err = syscall.Write(wfd, []byte("hello"))
	MustNil(t, err)

	// queue a wakeup task, then stop the poll before the loop runs
	MustNil(t, p.Trigger())
	Equal(t, atomic.LoadInt32(&p.queued), int32(1))
	MustNil(t, p.Close())

	// fetch the queued events (connection readable + wakeup) and handle them
	events := make([]epollevent, 8)
	var closed bool
	for i := 0; i < 3 && !closed; i++ {
		n, err := epollWaitUntil(p.fd, events, 1000)
		MustNil(t, err)
		closed = p.handler(events[:n])
	}
	// the wakeup event finishes the exit
	Assert(t, closed)
	// the queued connection event must not run its callback
	Equal(t, atomic.LoadInt32(&reads), int32(0))
	// queued wakeup tasks are drained
	Equal(t, atomic.LoadInt32(&p.queued), int32(0))
}

// Hammer Trigger/Control/Close concurrently: the exit must stay bounded and
// no task may be accepted after the poll stops.
func TestPollTriggerCloseRace(t *testing.T) {
	p, err := openDefaultPoll()
	MustNil(t, err)

	rfd, wfd := GetSysFdPairs()
	defer syscall.Close(rfd)
	defer syscall.Close(wfd)
	rop := &FDOperator{FD: rfd, OnRead: func(p Poll) error { return nil }, OnWrite: func(p Poll) error { return nil }}
	MustNil(t, p.Control(rop, PollReadable))

	waitErr := make(chan error, 1)
	go func() { waitErr <- p.Wait() }()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = p.Trigger()
				_ = p.Control(rop, PollR2RW)
				_ = p.Control(rop, PollRW2R)
			}
		}()
	}
	wg.Add(1)
	var closeErr error
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		closeErr = p.Close()
	}()
	wg.Wait()
	MustNil(t, closeErr)

	select {
	case err := <-waitErr:
		MustNil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("poll.Wait did not exit in time after Close")
	}
	<-p.Done()
	// tasks submitted after the exit are rejected
	Assert(t, errors.Is(p.Trigger(), ErrClosed))
}

// Repeated open/wait/close cycles must release the wakeup fd, the epoll fd
// and the Wait goroutine every time.
func TestPollCloseNoLeak(t *testing.T) {
	countFDs := func() int {
		ents, err := os.ReadDir("/proc/self/fd")
		MustNil(t, err)
		return len(ents)
	}

	beforeFDs := countFDs()
	beforeGs := runtime.NumGoroutine()
	const cycles = 20
	for i := 0; i < cycles; i++ {
		p, err := openDefaultPoll()
		MustNil(t, err)
		done := make(chan error, 1)
		go func() { done <- p.Wait() }()
		MustNil(t, p.Close())
		select {
		case err := <-done:
			MustNil(t, err)
		case <-time.After(time.Second):
			t.Fatalf("cycle %d: poll.Wait did not exit in time", i)
		}
		<-p.Done()
	}
	// no fd leak: wakeup fd and epoll fd are released before Done is closed
	Equal(t, countFDs(), beforeFDs)
	// no goroutine leak: all Wait goroutines exit after Done is closed
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > beforeGs && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	Assert(t, runtime.NumGoroutine() <= beforeGs, runtime.NumGoroutine(), beforeGs)
}
