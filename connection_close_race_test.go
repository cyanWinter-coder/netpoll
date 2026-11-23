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
	"testing"
	"time"
)

// newCloseRacePair builds two registered connections backed by a socketpair
// with small socket buffers so a large write can block in the poller wait.
func newCloseRacePair(t *testing.T) (reader, writer *connection) {
	t.Helper()
	r, w := GetSysFdPairs()
	setCloseTestSocketBuffers(t, r)
	setCloseTestSocketBuffers(t, w)
	reader = &connection{}
	writer = &connection{}
	if err := reader.init(&netFD{fd: r}, nil); err != nil {
		t.Fatalf("init reader: %v", err)
	}
	if err := writer.init(&netFD{fd: w}, nil); err != nil {
		t.Fatalf("init writer: %v", err)
	}
	t.Cleanup(func() {
		reader.Close()
		writer.Close()
	})
	return reader, writer
}

func setCloseTestSocketBuffers(t *testing.T, fd int) {
	t.Helper()
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096); err != nil {
		t.Fatalf("setsockopt sndbuf: %v", err)
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096); err != nil {
		t.Fatalf("setsockopt rcvbuf: %v", err)
	}
}

// TestCloseRejectsNewRead proves that once closing starts a read that has no
// buffered data is rejected with the terminal error instead of dangling.
func TestCloseRejectsNewRead(t *testing.T) {
	reader, _ := newCloseRacePair(t)

	errs := make(chan error, 1)
	go func() {
		_, e := reader.Reader().Next(1)
		errs <- e
	}()

	// let the reader park first, then close.
	deadline := time.Now().Add(time.Second)
	for !reader.isReadParkedForTest() {
		if time.Now().After(deadline) {
			t.Fatal("reader never parked")
		}
		time.Sleep(time.Millisecond)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-errs:
		if !errors.Is(err, ErrConnClosed) && !errors.Is(err, ErrEOF) {
			t.Fatalf("read after close err = %v, want terminal error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read goroutine hung after close")
	}

	// a brand new read after Close must fail immediately and never block.
	done := make(chan error, 1)
	go func() {
		_, e := reader.Reader().Next(1)
		done <- e
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("new read after Close succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("new read after Close was not rejected")
	}
}

// TestCloseWhileFlushBlocked proves a flush parked on a full socket is
// terminated with ErrConnClosed and never reports success after Close.
func TestCloseWhileFlushBlocked(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		_, writer := newCloseRacePair(t)

		payload := make([]byte, 4*1024*1024)
		flushErr := make(chan error, 1)
		go func() {
			_, e := writer.Write(payload)
			flushErr <- e
		}()

		deadline := time.Now().Add(2 * time.Second)
		for writer.outputBuffer.IsEmpty() {
			if time.Now().After(deadline) {
				t.Fatalf("iter %d: writer never queued data", iter)
			}
			time.Sleep(time.Millisecond)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("iter %d: close: %v", iter, err)
		}
		select {
		case err := <-flushErr:
			if err == nil {
				t.Fatalf("iter %d: flush reported success after Close", iter)
			}
			if !errors.Is(err, ErrConnClosed) {
				t.Fatalf("iter %d: flush err = %v, want ErrConnClosed", iter, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: flush goroutine hung after Close", iter)
		}
	}
}

// TestCloseConcurrentWithReadWrite registers many active read/write calls and
// closes concurrently, asserting every call returns exactly once and the
// process has no leaked goroutine waiting on a trigger channel.
func TestCloseConcurrentWithReadWrite(t *testing.T) {
	const goroutines = 32
	for iter := 0; iter < 10; iter++ {
		reader, writer := newCloseRacePair(t)
		var wg sync.WaitGroup
		wg.Add(goroutines * 2)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				_, _ = reader.Reader().Next(1)
			}()
			go func() {
				defer wg.Done()
				_, _ = writer.Write(make([]byte, 4*1024*1024))
			}()
		}

		// give the calls time to register, then close from another goroutine.
		time.Sleep(5 * time.Millisecond)
		if err := reader.Close(); err != nil {
			t.Fatalf("iter %d: close reader: %v", iter, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("iter %d: close writer: %v", iter, err)
		}

		finished := make(chan struct{})
		go func() {
			wg.Wait()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Fatalf("iter %d: active calls leaked after close", iter)
		}
	}
}

// TestRepeatedCloseIsIdempotent asserts repeated Close returns nil, runs the
// close callbacks exactly once and never double-frees the operator/buffers.
func TestRepeatedCloseIsIdempotent(t *testing.T) {
	reader, writer := newCloseRacePair(t)

	var readCallbacks, writeCallbacks int32
	if err := reader.AddCloseCallback(func(Connection) error {
		atomic.AddInt32(&readCallbacks, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddCloseCallback(func(Connection) error {
		atomic.AddInt32(&writeCallbacks, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const times = 20
	var wg sync.WaitGroup
	wg.Add(times * 2)
	for i := 0; i < times; i++ {
		go func() {
			defer wg.Done()
			if err := reader.Close(); err != nil {
				t.Errorf("reader Close returned %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := writer.Close(); err != nil {
				t.Errorf("writer Close returned %v", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&readCallbacks); got != 1 {
		t.Fatalf("reader close callbacks ran %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&writeCallbacks); got != 1 {
		t.Fatalf("writer close callbacks ran %d times, want 1", got)
	}
	if reader.IsActive() || writer.IsActive() {
		t.Fatal("connections should be inactive after Close")
	}
}

// TestCloseDuringReadTimeout proves a reader parked with a timeout is woken by
// the close broadcast and returns the terminal error rather than waiting out
// its full deadline.
func TestCloseDuringReadTimeout(t *testing.T) {
	reader, _ := newCloseRacePair(t)
	if err := reader.SetReadTimeout(time.Minute); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 1)
	go func() {
		_, e := reader.Reader().Next(1)
		errs <- e
	}()

	deadline := time.Now().Add(time.Second)
	for !reader.isReadParkedForTest() {
		if time.Now().After(deadline) {
			t.Fatal("reader never parked")
		}
		time.Sleep(time.Millisecond)
	}

	start := time.Now()
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-errs:
		if !errors.Is(err, ErrConnClosed) && !errors.Is(err, ErrEOF) {
			t.Fatalf("read err = %v, want terminal error", err)
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("read was not cancelled promptly by close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed-out read hung after close")
	}
}

// TestCloseDuringWriteTimeout exercises the timed waitFlush branch: a blocked
// flush must be cancelled by Close with ErrConnClosed rather than waiting out
// the write deadline.
func TestCloseDuringWriteTimeout(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		_, writer := newCloseRacePair(t)
		if err := writer.SetWriteTimeout(time.Minute); err != nil {
			t.Fatal(err)
		}

		flushErr := make(chan error, 1)
		go func() {
			_, e := writer.Write(make([]byte, 4*1024*1024))
			flushErr <- e
		}()

		deadline := time.Now().Add(2 * time.Second)
		for writer.outputBuffer.IsEmpty() {
			if time.Now().After(deadline) {
				t.Fatalf("iter %d: writer never queued data", iter)
			}
			time.Sleep(time.Millisecond)
		}

		start := time.Now()
		if err := writer.Close(); err != nil {
			t.Fatalf("iter %d: close: %v", iter, err)
		}
		select {
		case err := <-flushErr:
			if err == nil {
				t.Fatalf("iter %d: timed flush succeeded after Close", iter)
			}
			if !errors.Is(err, ErrConnClosed) {
				t.Fatalf("iter %d: timed flush err = %v, want ErrConnClosed", iter, err)
			}
			if time.Since(start) > 5*time.Second {
				t.Fatalf("iter %d: flush not cancelled promptly", iter)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("iter %d: timed flush hung after Close", iter)
		}
	}
}

// isReadParkedForTest reports whether a read wait is currently registered,
// i.e. the reader has passed beginReadWait and is selecting on its
// trigger/done channels.
func (c *connection) isReadParkedForTest() bool {
	return atomic.LoadInt32(&c.readWaiters) > 0
}
