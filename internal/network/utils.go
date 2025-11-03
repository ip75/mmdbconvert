// Package network provides IP address and network utilities for MMDB processing.
package network

import (
	"encoding/binary"
	"net/netip"
)

// IPv4ToUint32 converts an IPv4 address to uint32.
func IPv4ToUint32(addr netip.Addr) uint32 {
	if !addr.Is4() {
		panic("IPv4ToUint32 called with non-IPv4 address")
	}
	bytes := addr.As4()
	return binary.BigEndian.Uint32(bytes[:])
}

// IsAdjacent checks if two IP addresses are consecutive (no gap between them).
func IsAdjacent(endIP, startIP netip.Addr) bool {
	if endIP.Is4() != startIP.Is4() {
		return false
	}
	return endIP.Next() == startIP
}

// SmallestNetwork returns the smaller (more specific) of two overlapping network prefixes.
func SmallestNetwork(a, b netip.Prefix) netip.Prefix {
	// The network with more bits (longer prefix length) is more specific
	if a.Bits() >= b.Bits() {
		return a
	}
	return b
}
