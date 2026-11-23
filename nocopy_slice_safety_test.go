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
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func waitBufferLen(t *testing.T, c *connection, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for c.inputBuffer.Len() != want {
		if time.Now().After(deadline) {
			t.Fatalf("input buffer len=%d, want %d", c.inputBuffer.Len(), want)
		}
		runtime.Gosched()
	}
}

func waitBufferLenAtLeast(t *testing.T, c *connection, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for c.inputBuffer.Len() < want {
		if time.Now().After(deadline) {
			t.Fatalf("input buffer len=%d, want at least %d", c.inputBuffer.Len(), want)
		}
		runtime.Gosched()
	}
}

func waitConnClosed(t *testing.T, c *connection) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for c.IsActive() {
		if time.Now().After(deadline) {
			t.Fatal("connection was not closed after over-limit input")
		}
		runtime.Gosched()
	}
}

func waitWaitReadSize(t *testing.T, c *connection, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt64(&c.waitReadSize) != want {
		if time.Now().After(deadline) {
			t.Fatalf("waitReadSize=%d, want %d", atomic.LoadInt64(&c.waitReadSize), want)
		}
		runtime.Gosched()
	}
}

func mustWrite(t *testing.T, fd int, p []byte) {
	t.Helper()
	n, err := syscall.Write(fd, p)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(p) {
		t.Fatalf("write n=%d, want %d", n, len(p))
	}
}

// TestLinkBufferNextSliceSurvivesGrowth pins down requirement 1 & 2:
// a zero-copy slice returned by Next belongs to one read transaction. Buffer
// growth (new nodes appended on the write tail) must neither move nor overwrite
// the already exposed bytes, and incremental reads must keep the exposed view
// stable until Release.
func TestLinkBufferNextSliceSurvivesGrowth(t *testing.T) {
	LinkBufferCap = 128
	buf := NewLinkBuffer(128)

	// first generation: 64 bytes in node1
	p, err := buf.Malloc(64)
	MustNil(t, err)
	for i := range p {
		p[i] = byte(i)
	}
	buf.Flush()

	// expose the first 32 bytes zero-copy
	view, err := buf.Next(32)
	MustNil(t, err)
	Equal(t, len(view), 32)
	node1 := buf.head
	MustTrue(t, node1.readExposed())

	// grow with several more generations of nodes
	for g := 0; g < 4; g++ {
		q, err := buf.Malloc(128)
		MustNil(t, err)
		for i := range q {
			q[i] = byte(g+1) * 7
		}
		buf.Flush()
	}

	// the exposed view is untouched: growth appended nodes, never moved bytes
	for i := 0; i < 32; i++ {
		Equal(t, view[i], byte(i))
	}
	// unconsumed data right after the view is still where it was
	peek, err := buf.Peek(32)
	MustNil(t, err)
	for i := 0; i < 32; i++ {
		Equal(t, peek[i], byte(i+32))
	}

	// Release ends the read transaction: view memory goes back to the pool
	MustNil(t, buf.Release())
}

// TestLinkBufferPeekStableAcrossIncrementalRead verifies requirement 2 for the
// Peek path: repeated incremental fills keep the previously exposed prefix at
// the same address with the same content.
func TestLinkBufferPeekStableAcrossIncrementalRead(t *testing.T) {
	LinkBufferCap = 128
	buf := NewLinkBuffer(128)

	first, err := buf.Malloc(64)
	MustNil(t, err)
	for i := range first {
		first[i] = byte(i)
	}
	buf.Flush()

	view, err := buf.Peek(64)
	MustNil(t, err)
	viewPtr := &view[0]

	// fill more data incrementally without consuming the exposed bytes
	for g := 0; g < 3; g++ {
		more, err := buf.Malloc(64)
		MustNil(t, err)
		for i := range more {
			more[i] = byte(100 + g)
		}
		buf.Flush()
	}

	again, err := buf.Peek(64)
	MustNil(t, err)
	// same backing storage, same bytes: exposed bytes were never moved
	MustTrue(t, &again[0] == viewPtr)
	for i := 0; i < 64; i++ {
		Equal(t, again[i], byte(i))
	}
	MustNil(t, buf.Release())
}

// TestLinkBufferSliceHoldsReferenceUntilRelease covers requirement 3:
// Slice takes a reference on the origin node. The origin buffer cannot return
// to the pool while the slice reader still borrows it, even if the parent
// buffer has released its own reference.
func TestLinkBufferSliceHoldsReferenceUntilRelease(t *testing.T) {
	LinkBufferCap = 128
	buf := NewLinkBuffer(128)

	p, err := buf.Malloc(64)
	MustNil(t, err)
	for i := range p {
		p[i] = byte(i + 1)
	}
	buf.Flush()

	sub, err := buf.Slice(64)
	MustNil(t, err)
	MustNil(t, buf.Release())

	// the parent no longer references the node, but the slice keeps origin.refer > 1
	origin := sub.(*LinkBuffer).head.origin
	MustTrue(t, atomic.LoadInt32(&origin.refer) > 1)

	// data remains readable through the slice after the parent released
	got, err := sub.Next(64)
	MustNil(t, err)
	for i := 0; i < 64; i++ {
		Equal(t, got[i], byte(i+1))
	}
	MustNil(t, sub.Release())

	// once the slice drops its borrow, the origin is back to a single reference
	Equal(t, atomic.LoadInt32(&origin.refer), int32(1))
}

// TestLinkBufferReleaseReturnsMemoryOnlyWhenBorrowsZero is the pool-level view
// of requirement 3: an exposed, consumed node stays out of the mcache pool
// until Release; once Release runs and the node is detached from the chain,
// its buffer is actually recycled.
func TestLinkBufferReleaseReturnsMemoryOnlyWhenBorrowsZero(t *testing.T) {
	LinkBufferCap = 64
	buf := NewLinkBuffer(64)

	// node1
	p, err := buf.Malloc(64)
	MustNil(t, err)
	for i := range p {
		p[i] = byte(0xAB)
	}
	buf.Flush()
	// node2
	p, err = buf.Malloc(64)
	MustNil(t, err)
	for i := range p {
		p[i] = byte(0xCD)
	}
	buf.Flush()

	// consume node1 zero-copy, exposing it
	view, err := buf.Next(64)
	MustNil(t, err)
	Equal(t, view[0], byte(0xAB))
	exposedNode := buf.head
	MustTrue(t, exposedNode.readExposed())

	// advance the read head into node2 so node1 becomes a detached candidate
	dst := make([]byte, 64)
	n := buf.readCopy(dst)
	Equal(t, n, 64)
	Equal(t, dst[0], byte(0xCD))

	// while the exposed node is still in the chain, its buffer is retained
	MustTrue(t, exposedNode.buf != nil)
	MustNil(t, buf.Release())
	// Release detached the exposed node and returned its buffer to the pool
	MustTrue(t, exposedNode.buf == nil)
	MustTrue(t, buf.head != exposedNode)
}

// TestLinkBufferForceResetDiscardsGeneration covers requirement 4 at the
// buffer layer: forceReset invalidates every slice of the current generation
// and recycles the buffers, leaving a clean empty chain ready for reuse.
func TestLinkBufferForceResetDiscardsGeneration(t *testing.T) {
	LinkBufferCap = 128
	buf := NewLinkBuffer(128)

	for g := 0; g < 3; g++ {
		p, err := buf.Malloc(128)
		MustNil(t, err)
		for i := range p {
			p[i] = byte(g + 1)
		}
		buf.Flush()
	}
	view, err := buf.Next(32)
	MustNil(t, err)
	Equal(t, len(view), 32)
	MustTrue(t, buf.Len() > 0)

	buf.forceReset()

	// current generation is fully gone
	Equal(t, buf.Len(), 0)
	MustTrue(t, buf.IsEmpty())
	MustTrue(t, buf.head == buf.read && buf.read == buf.flush && buf.flush == buf.write)
	MustTrue(t, buf.head.next == nil)

	// the buffer is reusable: a fresh generation works normally
	p, err := buf.Malloc(16)
	MustNil(t, err)
	for i := range p {
		p[i] = byte(0x7F)
	}
	buf.Flush()
	got, err := buf.Next(16)
	MustNil(t, err)
	for i := range got {
		Equal(t, got[i], byte(0x7F))
	}
	MustNil(t, buf.Release())
	MustNil(t, buf.Close())
}

// TestLinkBufferReadCopyDoesNotExposeAndKeepsExposedBorrows re-affirms
// requirement 5 for the plain Reader path: copy reads never expose memory, so
// their nodes recycle immediately, while a previously exposed node is retained
// until the explicit Release.
func TestLinkBufferReadCopyDoesNotExposeAndKeepsExposedBorrows(t *testing.T) {
	LinkBufferCap = 64
	buf := NewLinkBuffer(64)

	// node1: exposed via Peek
	p, _ := buf.Malloc(64)
	for i := range p {
		p[i] = byte(i)
	}
	buf.Flush()
	// node2: only ever consumed via readCopy
	p, _ = buf.Malloc(64)
	for i := range p {
		p[i] = byte(i + 64)
	}
	buf.Flush()

	view, err := buf.Peek(8)
	MustNil(t, err)
	exposedNode := buf.head
	MustTrue(t, exposedNode.readExposed())

	dst := make([]byte, 128)
	n := buf.readCopy(dst)
	Equal(t, n, 128)
	for i := 0; i < 128; i++ {
		Equal(t, dst[i], byte(i))
	}
	// exposed view still valid after the copy read consumed its node
	for i := range view {
		Equal(t, view[i], byte(i))
	}
	// exposed node retained, non-exposed node already recycled
	Equal(t, buf.head, exposedNode)

	MustNil(t, buf.Release())
	MustTrue(t, buf.head != exposedNode)
}

// TestWithMaxInputLengthOption verifies the option carries the configured
// limit into the connection options consumed by onPrepare.
func TestWithMaxInputLengthOption(t *testing.T) {
	opts := &options{}
	WithMaxInputLength(4096).f(opts)
	Equal(t, opts.maxInputLength, 4096)

	// zero/negative values keep the default "unlimited" behavior
	WithMaxInputLength(0).f(opts)
	Equal(t, opts.maxInputLength, 0)
}

// TestConnInitAppliesMaxInputLengthOption exercises the full onPrepare path:
// a connection initialized with the option must have its atomic limit set
// before it is registered with the poller.
func TestConnInitAppliesMaxInputLengthOption(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	opts := &options{}
	WithMaxInputLength(256).f(opts)
	MustNil(t, c.init(&netFD{fd: r}, opts))
	Equal(t, atomic.LoadInt32(&c.maxInputLen), int32(256))
	MustNil(t, c.Close())
	MustNil(t, syscall.Close(w))
}

// TestConnInputExceededStableError drives requirement 4 end to end on a real
// connection: an oversized read produces the stable ErrConnInputExceeded error
// for the blocked reader, the current buffer generation is cleaned and the
// connection is closed.
func TestConnInputExceededStableError(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	MustNil(t, c.init(&netFD{fd: r}, nil))
	atomic.StoreInt32(&c.maxInputLen, 128)

	// first read within the limit
	mustWrite(t, w, make([]byte, 64))
	waitBufferLen(t, c, 64)
	Equal(t, c.Len(), 64)

	// reader blocks waiting for more data
	done := make(chan error, 1)
	go func() {
		_, err := c.Next(256)
		done <- err
	}()
	waitWaitReadSize(t, c, 256)

	// second read pushes unconsumed input past the limit
	mustWrite(t, w, make([]byte, 128))

	// the blocked reader receives the stable sentinel error
	select {
	case err := <-done:
		MustTrue(t, errors.Is(err, ErrConnInputExceeded))
	case <-time.After(time.Second):
		t.Fatal("blocked reader was not woken by over-limit error")
	}

	// current generation cleaned; the connection then closes asynchronously
	// via the poller hang-up path, so wait for that to complete.
	Equal(t, c.inputBuffer.Len(), 0)
	waitConnClosed(t, c)
	MustTrue(t, !c.IsActive())

	// any subsequent read keeps reporting the same stable terminal cause
	next, err2 := c.Next(1)
	MustTrue(t, len(next) == 0)
	MustTrue(t, err2 != nil)
	MustTrue(t, errors.Is(err2, ErrConnInputExceeded))

	MustNil(t, syscall.Close(w))
}

// TestConnInputExceededRecyclesAndReaderWakes checks that the over-limit error
// is delivered to a reader already blocked in waitRead, and that the reset
// buffer is empty rather than retaining the offending generation.
func TestConnInputExceededRecyclesAndReaderWakes(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	MustNil(t, c.init(&netFD{fd: r}, nil))
	atomic.StoreInt32(&c.maxInputLen, 64)

	done := make(chan error, 1)
	go func() {
		_, err := c.Next(256) // blocks until the limit trips
		done <- err
	}()
	waitWaitReadSize(t, c, 256)

	mustWrite(t, w, make([]byte, 128))

	select {
	case err := <-done:
		MustTrue(t, errors.Is(err, ErrConnInputExceeded))
	case <-time.After(time.Second):
		t.Fatal("blocked reader was not woken by over-limit error")
	}
	Equal(t, c.inputBuffer.Len(), 0)
	// the poller hang-up that closes the connection runs asynchronously
	waitConnClosed(t, c)
	MustTrue(t, !c.IsActive())
	MustNil(t, syscall.Close(w))
}

// TestConnInputLimitDisabledByDefault ensures zero value preserves the
// historical behavior: large unconsumed input never raises the new error.
func TestConnInputLimitDisabledByDefault(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	MustNil(t, c.init(&netFD{fd: r}, nil))
	Equal(t, atomic.LoadInt32(&c.maxInputLen), int32(0))

	mustWrite(t, w, make([]byte, 256))
	waitBufferLen(t, c, 256)
	Equal(t, c.Len(), 256)
	MustTrue(t, c.IsActive())

	MustNil(t, c.Close())
	MustNil(t, syscall.Close(w))
}

// TestConnInputLimitExactBoundary verifies the gate trips only when length is
// strictly greater than the configured limit.
func TestConnInputLimitExactBoundary(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	MustNil(t, c.init(&netFD{fd: r}, nil))
	atomic.StoreInt32(&c.maxInputLen, 128)

	mustWrite(t, w, make([]byte, 128))
	waitBufferLen(t, c, 128)
	MustTrue(t, c.IsActive()) // equal to limit is still allowed

	mustWrite(t, w, make([]byte, 1))
	waitConnClosed(t, c)
	MustTrue(t, !c.IsActive())
	MustNil(t, syscall.Close(w))
}

// TestConnInputExceededDetachesOperator proves the poller releases the
// operator cleanly on the over-limit path: the connection reaches the closed
// state (the hang-up path ran) and a subsequent user Close does not block.
// It pairs with TestConnInputExceededDoesNotStallPoller to confirm the poller
// goroutine itself keeps running rather than spinning inside operator.unused.
func TestConnInputExceededDetachesOperator(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	MustNil(t, c.init(&netFD{fd: r}, nil))
	atomic.StoreInt32(&c.maxInputLen, 128)

	mustWrite(t, w, make([]byte, 200))
	waitConnClosed(t, c)

	// the hang-up path completed and published the stable terminal error
	_, err := c.Next(1)
	MustTrue(t, errors.Is(err, ErrConnInputExceeded))
	MustNil(t, syscall.Close(w))
}

// TestConnInputExceededDoesNotStallPoller ensures one over-limit connection
// does not wedge the shared poller: a second connection on the same poller
// keeps receiving data afterwards.
func TestConnInputExceededDoesNotStallPoller(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	MustNil(t, c.init(&netFD{fd: r}, nil))
	atomic.StoreInt32(&c.maxInputLen, 128)
	mustWrite(t, w, make([]byte, 200))
	waitConnClosed(t, c)

	// a second connection on (likely) the same poller must still process reads
	r2, w2 := GetSysFdPairs()
	c2 := &connection{}
	MustNil(t, c2.init(&netFD{fd: r2}, nil))
	mustWrite(t, w2, make([]byte, 64))
	waitBufferLen(t, c2, 64)
	Equal(t, c2.Len(), 64)

	MustNil(t, c2.Close())
	MustNil(t, syscall.Close(w))
	MustNil(t, syscall.Close(w2))
}

// TestConnInputExceededUserCloseNotBlocked verifies that after the over-limit
// auto-close, an explicit user Close returns instead of blocking forever.
func TestConnInputExceededUserCloseNotBlocked(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	MustNil(t, c.init(&netFD{fd: r}, nil))
	atomic.StoreInt32(&c.maxInputLen, 128)
	mustWrite(t, w, make([]byte, 200))
	waitConnClosed(t, c)

	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("user Close blocked after over-limit auto-close")
	}
	MustNil(t, syscall.Close(w))
}

// TestConnGrowthWithHeldSlice is the connection-level version of requirement
// 1 & 2: a slice held across several poller-driven incremental reads keeps its
// bytes until Release even while the input buffer grows new nodes.
func TestConnGrowthWithHeldSlice(t *testing.T) {
	r, w := GetSysFdPairs()
	c := &connection{}
	MustNil(t, c.init(&netFD{fd: r}, nil))

	msg := make([]byte, 64)
	for i := range msg {
		msg[i] = byte(i)
	}
	mustWrite(t, w, msg)
	waitBufferLen(t, c, 64)

	view, err := c.Next(32)
	MustNil(t, err)
	Equal(t, len(view), 32)

	// several more reads grow the buffer while the view is held
	for g := 0; g < 4; g++ {
		chunk := make([]byte, 128)
		for i := range chunk {
			chunk[i] = byte(g + 1)
		}
		mustWrite(t, w, chunk)
		waitBufferLenAtLeast(t, c, 64+128*(g+1)-32)
	}
	for i := 0; i < 32; i++ {
		Equal(t, view[i], byte(i))
	}
	MustNil(t, c.Release())
	MustNil(t, c.Close())
	MustNil(t, syscall.Close(w))
}
