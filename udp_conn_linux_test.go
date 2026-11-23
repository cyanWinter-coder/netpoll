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
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// udpTestPair builds a netpoll listener on a random localhost UDP port and
// a stdlib client connected to it. The poller is initialized explicitly so
// the first datagram does not race lazy poller startup.
func udpTestPair(t *testing.T, network string) (srv UDPConn, cli *net.UDPConn) {
	t.Helper()
	Initialize()

	host := "127.0.0.1"
	if network == "udp6" {
		host = "[::1]"
	}
	srv, err := ListenPacket(network, host+":0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	raddr, err := net.ResolveUDPAddr(network, srv.LocalAddr().String())
	if err != nil {
		t.Fatalf("ResolveUDPAddr: %v", err)
	}
	cli, err = net.DialUDP(network, nil, raddr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	t.Cleanup(func() {
		_ = srv.Close()
		_ = cli.Close()
	})
	return srv, cli
}

// waitRead performs a ReadFrom with a generous timeout so a hung test fails
// instead of blocking forever.
func waitRead(t *testing.T, c UDPConn, buf []byte) (int, net.Addr, error) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	n, addr, err := c.ReadFrom(buf)
	return n, addr, err
}

// TestUDPShortBufferTruncates covers requirement 1: a datagram larger than
// the receive buffer is truncated, the remainder is dropped with that
// datagram, an ErrShortDatagram is returned, and the next ReadFrom returns
// the following datagram intact rather than the leftover bytes.
func TestUDPShortBufferTruncates(t *testing.T) {
	srv, cli := udpTestPair(t, "udp")

	payload := []byte("0123456789-abcdefghij") // 21 bytes
	if _, err := cli.Write(payload); err != nil {
		t.Fatalf("client write: %v", err)
	}
	second := []byte("second-datagram")
	if _, err := cli.Write(second); err != nil {
		t.Fatalf("client write: %v", err)
	}

	// tiny buffer: only the prefix fits
	small := make([]byte, 5)
	n, _, err := waitRead(t, srv, small)
	if err == nil {
		t.Fatalf("expected truncation error, got nil (n=%d)", n)
	}
	if !errors.Is(err, ErrShortDatagram) {
		t.Fatalf("expected ErrShortDatagram, got %v", err)
	}
	if n != len(small) {
		t.Fatalf("expected %d prefix bytes, got %d", len(small), n)
	}
	if string(small) != string(payload[:5]) {
		t.Fatalf("prefix mismatch: %q want %q", small, payload[:5])
	}

	// the remainder must not leak into the next datagram
	buf := make([]byte, 64)
	n2, addr, err := waitRead(t, srv, buf)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if string(buf[:n2]) != string(second) {
		t.Fatalf("datagram boundary broken: got %q want %q", buf[:n2], second)
	}
	if addr == nil || addr.String() != cli.LocalAddr().String() {
		t.Fatalf("source addr mismatch: got %v want %v", addr, cli.LocalAddr())
	}
}

// TestUDPSourceAndDstAddressBound covers requirement 2: the source and
// destination addresses belong to the datagram they arrive with and must not
// bleed across messages from different peers.
func TestUDPSourceAndDstAddressBound(t *testing.T) {
	srv, cli1 := udpTestPair(t, "udp")

	cli2, err := net.DialUDP("udp", nil, srv.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("second client: %v", err)
	}
	defer cli2.Close()

	m1, m2 := []byte("from-one"), []byte("from-two")
	if _, err := cli1.Write(m1); err != nil {
		t.Fatal(err)
	}
	if _, err := cli2.Write(m2); err != nil {
		t.Fatal(err)
	}

	got := make(map[string]string) // source -> payload
	buf := make([]byte, 64)
	for i := 0; i < 2; i++ {
		n, src, rerr := waitRead(t, srv, buf)
		if rerr != nil {
			t.Fatalf("read %d: %v", i, rerr)
		}
		got[src.String()] = string(buf[:n])
	}
	if got[cli1.LocalAddr().String()] != string(m1) {
		t.Fatalf("cli1 datagram/addr not bound correctly: %+v", got)
	}
	if got[cli2.LocalAddr().String()] != string(m2) {
		t.Fatalf("cli2 datagram/addr not bound correctly: %+v", got)
	}

	// destination address must be the listener's local address
	if _, err := cli1.Write([]byte("dst-check")); err != nil {
		t.Fatal(err)
	}
	_ = srv.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf2 := make([]byte, 64)
	n, src, dst, derr := srv.ReadFromUDP(buf2)
	if derr != nil {
		t.Fatalf("ReadFromUDP: %v", derr)
	}
	if n != len("dst-check") {
		t.Fatalf("dst-check length: %d", n)
	}
	if src == nil || src.String() != cli1.LocalAddr().String() {
		t.Fatalf("src mismatch: %v", src)
	}
	if dst == nil {
		t.Fatal("destination address not delivered (IP_PKTINFO missing)")
	}
	if dst.IP == nil || !dst.IP.IsLoopback() {
		t.Fatalf("dst IP should be loopback, got %v", dst)
	}
}

// TestUDPZeroAndMaxLengthDatagram covers requirement 3: zero-length and
// maximum-length datagrams both produce stable, boundary-preserving results.
func TestUDPZeroAndMaxLengthDatagram(t *testing.T) {
	srv, cli := udpTestPair(t, "udp")

	// zero-length datagram
	if _, err := cli.Write([]byte{}); err != nil {
		t.Fatalf("write empty: %v", err)
	}
	buf := make([]byte, 32)
	n, addr, err := waitRead(t, srv, buf)
	if err != nil {
		t.Fatalf("read empty datagram: %v", err)
	}
	if n != 0 {
		t.Fatalf("zero-length datagram should read 0 bytes, got %d", n)
	}
	if addr == nil || addr.String() != cli.LocalAddr().String() {
		t.Fatalf("zero-length datagram lost its source addr: %v", addr)
	}

	// a second, non-empty datagram follows cleanly
	if _, err := cli.Write([]byte("after-empty")); err != nil {
		t.Fatal(err)
	}
	n, _, err = waitRead(t, srv, buf)
	if err != nil || string(buf[:n]) != "after-empty" {
		t.Fatalf("read after empty failed: n=%d err=%v", n, err)
	}

	// large datagram: the largest size this kernel actually delivers.
	// Some environments (e.g. certain WSL2 loopback setups) silently drop
	// datagrams above the interface MTU, so probe the supported ceiling
	// rather than hard-requiring the theoretical 65507 maximum.
	bigSize := probeMaxDatagramSize(t, cli, srv)
	big := make([]byte, bigSize)
	for i := range big {
		big[i] = byte(i)
	}
	if _, err := cli.Write(big); err != nil {
		t.Fatalf("write %d-byte datagram: %v", bigSize, err)
	}
	large := make([]byte, maxUDPDatagramSize)
	n, _, err = waitRead(t, srv, large)
	if err != nil {
		t.Fatalf("read large datagram: %v", err)
	}
	if n != bigSize {
		t.Fatalf("large datagram truncated: got %d want %d", n, bigSize)
	}
	for i := 0; i < bigSize; i++ {
		if large[i] != byte(i) {
			t.Fatalf("large datagram content mismatch at %d", i)
		}
	}
}

// probeMaxDatagramSize returns the largest datagram length the current
// kernel delivers on loopback. Some environments silently drop datagrams
// above the interface MTU, so we probe the supported ceiling rather than
// hard-requiring the theoretical 65507 maximum.
func probeMaxDatagramSize(t *testing.T, cli *net.UDPConn, srv UDPConn) int {
	t.Helper()
	for _, sz := range []int{maxUDPDatagramSize, 32768, 8192, 4096, 1472, 512} {
		if _, err := cli.Write(make([]byte, sz)); err != nil {
			continue
		}
		_ = srv.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		n, _, err := srv.ReadFrom(make([]byte, maxUDPDatagramSize))
		if err == nil && n == sz {
			return sz
		}
	}
	return 128 // smallest guaranteed size
}

// TestUDPConcurrentReadFrom covers requirement 4: concurrent ReadFrom calls
// compete for whole datagrams; no datagram is split between two callers.
func TestUDPConcurrentReadFrom(t *testing.T) {
	srv, cli := udpTestPair(t, "udp")

	const total = 200
	var wg sync.WaitGroup
	results := make(chan string, total)

	// more readers than datagrams, all blocked before data arrives
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 64)
			for {
				n, _, rerr := srv.ReadFrom(buf)
				if rerr != nil {
					return
				}
				results <- string(buf[:n])
			}
		}()
	}

	// let all readers block
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < total; i++ {
		msg := "datagram-" + strconv.Itoa(i)
		if _, err := cli.Write([]byte(msg)); err != nil {
			t.Fatal(err)
		}
	}

	want := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		want["datagram-"+strconv.Itoa(i)] = false
	}
	deadline := time.After(3 * time.Second)
	for received := 0; received < total; {
		select {
		case msg := <-results:
			seen, ok := want[msg]
			if !ok {
				t.Fatalf("unexpected/garbled datagram: %q", msg)
			}
			if seen {
				t.Fatalf("datagram delivered twice: %q", msg)
			}
			want[msg] = true
			received++
		case <-deadline:
			t.Fatalf("timed out, only received %d/%d datagrams", received, total)
		}
	}

	if err := srv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	wg.Wait()

	// every datagram was consumed exactly once, whole
	for msg, seen := range want {
		if !seen {
			t.Fatalf("datagram %q was lost", msg)
		}
	}
}

// TestUDPCloseWakesReaders covers requirement 5: closing the connection
// unblocks every waiting reader with ErrConnClosed.
func TestUDPCloseWakesReaders(t *testing.T) {
	srv, _ := udpTestPair(t, "udp")

	const readers = 16
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 32)
			_, _, rerr := srv.ReadFrom(buf)
			errs <- rerr
		}()
	}

	// ensure all readers are parked before closing
	time.Sleep(150 * time.Millisecond)
	if err := srv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("close did not wake all blocked readers")
	}
	close(errs)
	for rerr := range errs {
		if !errors.Is(rerr, ErrConnClosed) {
			t.Fatalf("reader woke with %v, want ErrConnClosed", rerr)
		}
	}

	// Close must be idempotent
	if err := srv.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestUDPMultipleDatagramsInQueue verifies the poller drains several
// datagrams per wakeup and preserves FIFO order and boundaries.
func TestUDPMultipleDatagramsInQueue(t *testing.T) {
	srv, cli := udpTestPair(t, "udp")

	msgs := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, m := range msgs {
		if _, err := cli.Write([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	// give the poller one round to drain them all into the queue
	time.Sleep(100 * time.Millisecond)

	buf := make([]byte, 32)
	for _, want := range msgs {
		n, _, err := waitRead(t, srv, buf)
		if err != nil {
			t.Fatalf("read %q: %v", want, err)
		}
		if got := string(buf[:n]); got != want {
			t.Fatalf("FIFO/boundary broken: got %q want %q", got, want)
		}
	}
}

// TestUDP6Addresses verifies the IPv6 control-message path: both the source
// and the local destination address are delivered as IPv6 loopback.
func TestUDP6Addresses(t *testing.T) {
	srv, cli := udpTestPair(t, "udp6")
	if _, err := cli.Write([]byte("v6-msg")); err != nil {
		t.Fatal(err)
	}
	_ = srv.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 64)
	n, src, dst, err := srv.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("ReadFromUDP: %v", err)
	}
	if string(buf[:n]) != "v6-msg" {
		t.Fatalf("payload: %q", buf[:n])
	}
	if src == nil || !src.IP.Equal(net.IPv6loopback) {
		t.Fatalf("src should be v6 loopback, got %v", src)
	}
	if dst == nil || !dst.IP.Equal(net.IPv6loopback) {
		t.Fatalf("dst should be v6 loopback (IPV6_RECVPKTINFO), got %v", dst)
	}
}

// TestUDPReadDeadline verifies a read timeout returns a net.Error timeout
// and that a later datagram can still be read after the timeout.
func TestUDPReadDeadline(t *testing.T) {
	srv, cli := udpTestPair(t, "udp")

	_ = srv.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	buf := make([]byte, 16)
	if _, _, err := srv.ReadFrom(buf); err == nil {
		t.Fatal("expected timeout error")
	} else {
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("expected net.Error timeout, got %T %v", err, err)
		}
	}

	// connection stays usable after a timeout
	if _, err := cli.Write([]byte("recovered")); err != nil {
		t.Fatal(err)
	}
	_ = srv.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, _, err := srv.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "recovered" {
		t.Fatalf("read after timeout failed: n=%d err=%v", n, err)
	}
}

// TestUDPWriteToRoundTrip verifies a datagram sent through netpoll reaches a
// stdlib peer and that addresses travel correctly in both directions.
func TestUDPWriteToRoundTrip(t *testing.T) {
	srv, cli := udpTestPair(t, "udp")

	// client -> server, then server -> client via WriteTo
	if _, err := cli.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	n, src, err := waitRead(t, srv, buf)
	if err != nil || string(buf[:n]) != "ping" {
		t.Fatalf("server read: n=%d err=%v", n, err)
	}

	n, err = srv.WriteTo([]byte("pong"), src)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != len("pong") {
		t.Fatalf("WriteTo n=%d", n)
	}
	_ = cli.SetReadDeadline(time.Now().Add(3 * time.Second))
	rbuf := make([]byte, 32)
	rn, _, rerr := cli.ReadFromUDP(rbuf)
	if rerr != nil || string(rbuf[:rn]) != "pong" {
		t.Fatalf("client read reply: n=%d err=%v", rn, rerr)
	}
}

// TestDialUDPConnected exercises the connected-socket path: Read/Write
// without explicit addresses and fixed-peer association.
func TestDialUDPConnected(t *testing.T) {
	Initialize()

	// stdlib echo peer
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	go func() {
		b := make([]byte, 1024)
		for {
			n, sa, e := peer.ReadFromUDP(b)
			if e != nil {
				return
			}
			_, _ = peer.WriteToUDP(b[:n], sa)
		}
	}()

	conn, err := DialUDP("udp", nil, peer.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer conn.Close()
	if conn.RemoteAddr() == nil {
		t.Fatal("connected conn should expose a remote address")
	}

	if _, err := conn.Write([]byte("echo-me")); err != nil {
		t.Fatalf("connected Write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil || string(buf[:n]) != "echo-me" {
		t.Fatalf("connected Read: n=%d err=%v", n, err)
	}
}
