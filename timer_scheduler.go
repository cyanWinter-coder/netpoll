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
	"container/heap"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// timerMaxDispatchPerTick bounds how many due timers a single scheduler tick
// may dispatch. When more timers than this expire on the same tick, the
// remainder are left in the heap and handled on the next tick so that a burst
// of short-deadline connections cannot starve every other fd of poll time.
const timerMaxDispatchPerTick = 1024

// timerDir identifies which direction a timer guards.
// Read and write timers are independent: updating one never invalidates the
// generation of the other.
type timerDir int

const (
	timerRead timerDir = iota
	timerWrite
)

// timerNode is one scheduled deadline inside a timerScheduler.
//
// A node carries the generation (deadline version) of its connection at the
// time it was scheduled. On every deadline update the connection bumps its
// generation and re-schedules, so a node whose generation is stale when it
// becomes due describes an already-superseded deadline and is dropped
// without firing. That makes deadline updates an overwritable, generational
// state: old events can never act on a newer deadline.
type timerNode struct {
	conn       *connection
	dir        timerDir
	deadline   int64 // UnixNano
	generation uint64
	index      int // heap index, maintained by container/heap; -1 when not in heap
}

// timerHeap orders nodes by deadline, FIFO among equal deadlines so that
// connections are served in the order their deadlines were registered.
type timerHeap []*timerNode

func (h timerHeap) Len() int { return len(h) }

func (h timerHeap) Less(i, j int) bool {
	if h[i].deadline != h[j].deadline {
		return h[i].deadline < h[j].deadline
	}
	// Tie-break by generation: within the same tick, earlier registrations
	// (smaller generation) are dispatched first. This keeps ordering fair and
	// deterministic instead of relying on pointer comparison.
	return h[i].generation < h[j].generation
}

func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *timerHeap) Push(x interface{}) {
	node := x.(*timerNode)
	node.index = len(*h)
	*h = append(*h, node)
}

func (h *timerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	node := old[n-1]
	old[n-1] = nil
	node.index = -1
	*h = old[:n-1]
	return node
}

// timerScheduler is a process-wide min-heap timer wheel. All connections share
// a single scheduler goroutine, which means short-deadline connections are
// ordered purely by deadline across the whole process and dispatched in fair
// batches rather than each connection owning an independent Go runtime timer.
type timerScheduler struct {
	mu       sync.Mutex
	heap     timerHeap
	wake     chan struct{}
	started  int32
	nodePool sync.Pool
}

var globalTimerScheduler = newTimerScheduler()

func newTimerScheduler() *timerScheduler {
	s := &timerScheduler{
		wake: make(chan struct{}, 1),
	}
	s.heap = make(timerHeap, 0, 64)
	heap.Init(&s.heap)
	s.nodePool = sync.Pool{
		New: func() interface{} { return new(timerNode) },
	}
	return s
}

// start launches the single scheduler goroutine once. It is safe to call
// concurrently; only the first call launches a loop.
func (s *timerScheduler) start() {
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		return
	}
	go s.loop()
}

// schedule inserts a deadline node into the heap and wakes the loop.
func (s *timerScheduler) schedule(node *timerNode) {
	s.mu.Lock()
	heap.Push(&s.heap, node)
	s.mu.Unlock()
	s.signal()
}

// remove deletes a still-pending node. It returns true when it owned and
// removed the node (the caller may then recycle it), and false when the node
// was never in the heap or had already been popped for dispatch. This makes
// node recycling single-owner: a node is put back to the pool exactly once.
//
// node.index is mutated by heap operations under s.mu, so every read of it
// must hold the lock; the initial "not in heap" check therefore lives inside
// the critical section rather than in a lock-free fast path.
func (s *timerScheduler) remove(node *timerNode) (removed bool) {
	if node == nil {
		return false
	}
	s.mu.Lock()
	if node.index >= 0 && node.index < s.heap.Len() && s.heap[node.index] == node {
		heap.Remove(&s.heap, node.index)
		removed = true
	}
	s.mu.Unlock()
	return removed
}

// len returns the current heap size; used by tests to assert no stale entries.
func (s *timerScheduler) len() int {
	s.mu.Lock()
	n := s.heap.Len()
	s.mu.Unlock()
	return n
}

// signal wakes the scheduler loop so it recomputes its sleep duration.
func (s *timerScheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *timerScheduler) loop() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		wait, due := s.takeDueBatch()
		if len(due) > 0 {
			// Dispatch outside the heap lock. The batch cap keeps a single
			// tick from monopolizing the scheduler and starving other fds.
			for _, node := range due {
				s.fire(node)
				s.nodePool.Put(node) // single owner: a popped node is recycled here
			}
			// Yield so the goroutine scheduler can run other work between
			// huge batches instead of spinning through every due timer.
			runtime.Gosched()
			continue
		}
		timer.Reset(wait)
		select {
		case <-timer.C:
		case <-s.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

// takeDueBatch returns the duration until the next deadline plus a capped
// batch of nodes whose deadline has passed. Nodes that cannot fit in the
// batch stay in the heap (they are already due, so they pop first next tick),
// which is what bounds the work of any single tick.
func (s *timerScheduler) takeDueBatch() (wait time.Duration, due []*timerNode) {
	now := time.Now().UnixNano()
	s.mu.Lock()
	for s.heap.Len() > 0 && len(due) < timerMaxDispatchPerTick {
		top := s.heap[0]
		if top.deadline > now {
			break
		}
		due = append(due, heap.Pop(&s.heap).(*timerNode))
	}
	if s.heap.Len() > 0 {
		wait = time.Duration(s.heap[0].deadline - now)
		if wait < 0 {
			wait = 0
		}
	} else {
		wait = time.Hour
	}
	s.mu.Unlock()
	return wait, due
}

// fire delivers a timeout to the owning connection, but only if the node still
// represents the current generation. Stale nodes (a deadline that was updated
// or cleared before it fired) are silently ignored.
func (s *timerScheduler) fire(node *timerNode) {
	c := node.conn
	if c == nil {
		return
	}
	c.timerMu.Lock()
	st := &c.timers[node.dir]
	curNode, curGen := st.node, st.generation
	c.timerMu.Unlock()
	if curNode == nil || curNode != node || curGen != node.generation {
		// superseded by a newer deadline or already removed
		return
	}
	switch node.dir {
	case timerRead:
		c.triggerRead(c.readTimeoutError())
	case timerWrite:
		// mirror the old write-timeout behavior: stop polling write events.
		// operator may be nil for detached/closed connections or scheduler
		// unit tests; the timeout notification is still delivered.
		if c.operator != nil {
			c.operator.Control(PollRW2R)
		}
		c.triggerWrite(c.writeTimeoutError())
	}
}

// readTimeoutError builds the read-timeout error without assuming the
// connection's remote address has been initialized (scheduler tests and
// partially constructed connections may leave it nil).
func (c *connection) readTimeoutError() error {
	if c.remoteAddr != nil {
		return Exception(ErrReadTimeout, c.remoteAddr.String())
	}
	return Exception(ErrReadTimeout, "")
}

// writeTimeoutError is the write-direction counterpart of readTimeoutError.
func (c *connection) writeTimeoutError() error {
	if c.remoteAddr != nil {
		return Exception(ErrWriteTimeout, c.remoteAddr.String())
	}
	return Exception(ErrWriteTimeout, "")
}

// allocNode fetches a reusable node and fills it.
func (s *timerScheduler) allocNode(c *connection, dir timerDir, deadline int64, generation uint64) *timerNode {
	node := s.nodePool.Get().(*timerNode)
	node.conn = c
	node.dir = dir
	node.deadline = deadline
	node.generation = generation
	node.index = -1
	return node
}
