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
	"context"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// ListenPacket announces a datagram socket on the given local address and
// returns a netpoll-driven UDPConn. The network must be "udp", "udp4" or
// "udp6". Unlike a stream listener, a UDP socket has no Accept loop: all
// peers share the single returned connection and are distinguished per
// datagram through the source address returned by ReadFrom.
func ListenPacket(network, address string) (UDPConn, error) {
	switch network {
	case "udp", "udp4", "udp6":
	default:
		return nil, net.UnknownNetworkError(network)
	}

	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	pc, err := net.ListenUDP(network, addr)
	if err != nil {
		return nil, err
	}
	// capture before pc is closed below
	local, _ := pc.LocalAddr().(*net.UDPAddr)

	// Take ownership of a private, non-blocking, close-on-exec fd.
	// The stdlib packet conn is closed afterwards; dup keeps the socket
	// alive through our fd.
	file, err := pc.File()
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	fd, err := syscall.Dup(int(file.Fd()))
	_ = file.Close()
	_ = pc.Close()
	if err != nil {
		return nil, err
	}
	if err = syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	syscall.CloseOnExec(fd)

	if err = enableDstAddr(fd, network); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}

	return newUDPConn(fd, network, local)
}

// DialUDP connects to the remote address raddr on the network and returns a
// netpoll-driven UDPConn bound to a single peer. On such a connection Read
// receives only datagrams from that peer and Write sends to it without an
// explicit destination address.
func DialUDP(network string, laddr, raddr *net.UDPAddr) (UDPConn, error) {
	switch network {
	case "udp", "udp4", "udp6":
	default:
		return nil, net.UnknownNetworkError(network)
	}
	if raddr == nil {
		return nil, &net.OpError{Op: "dial", Net: network, Source: laddr, Addr: nil, Err: errMissingAddress}
	}

	d := net.Dialer{LocalAddr: laddr}
	conn, err := d.DialContext(context.Background(), network, raddr.String())
	if err != nil {
		return nil, err
	}
	uc, ok := conn.(*net.UDPConn)
	if !ok {
		_ = conn.Close()
		return nil, &net.OpError{Op: "dial", Net: network, Addr: raddr, Err: net.UnknownNetworkError(network)}
	}

	file, err := uc.File()
	if err != nil {
		_ = uc.Close()
		return nil, err
	}
	fd, err := syscall.Dup(int(file.Fd()))
	_ = file.Close()
	local, _ := conn.LocalAddr().(*net.UDPAddr)
	_ = uc.Close()
	if err != nil {
		return nil, err
	}
	if err = syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	syscall.CloseOnExec(fd)

	if err = enableDstAddr(fd, network); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}

	c, err := newUDPConn(fd, network, local)
	if err != nil {
		return nil, err
	}
	c.connected = true
	c.peerAddr = raddr
	return c, nil
}

// enableDstAddr turns on the control messages that carry the local
// destination address of each received datagram, so ReadFromUDP can report
// it alongside the source address.
//
// For the dual-stack "udp" network the concrete socket may be IPv4-only or
// IPv6-only depending on address resolution, so an option unsupported by
// the actual protocol family is tolerated rather than treated as fatal.
func enableDstAddr(fd int, network string) error {
	switch network {
	case "udp4":
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, unix.IP_PKTINFO, 1); err != nil {
			return err
		}
	case "udp6":
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, unix.IPV6_RECVPKTINFO, 1); err != nil {
			return err
		}
	default: // "udp": best effort, one family will accept its matching option
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, unix.IP_PKTINFO, 1); err != nil &&
			err != syscall.ENOPROTOOPT {
			return err
		}
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, unix.IPV6_RECVPKTINFO, 1); err != nil &&
			err != syscall.ENOPROTOOPT {
			return err
		}
	}
	return nil
}
