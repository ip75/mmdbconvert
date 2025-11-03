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

// IPv6ToBytes converts an IPv6 address to a 16-byte array.
func IPv6ToBytes(addr netip.Addr) [16]byte {
	if !addr.Is6() {
		panic("IPv6ToBytes called with non-IPv6 address")
	}
	return addr.As16()
}

// IPv4ToPaddedIPv6 pads an IPv4 address to 16 bytes for mixed Parquet files.
// The padding follows the IPv4-mapped IPv6 format.
func IPv4ToPaddedIPv6(addr netip.Addr) [16]byte {
	if !addr.Is4() {
		panic("IPv4ToPaddedIPv6 called with non-IPv4 address")
	}
	// Convert to IPv4-mapped IPv6 (::ffff:x.x.x.x)
	ipv4Bytes := addr.As4()
	var result [16]byte
	result[10] = 0xff
	result[11] = 0xff
	copy(result[12:], ipv4Bytes[:])
	return result
}

// IsAdjacent checks if two IP addresses are consecutive (no gap between them).
func IsAdjacent(endIP, startIP netip.Addr) bool {
	if endIP.Is4() != startIP.Is4() {
		return false
	}

	if endIP.Is4() {
		end := IPv4ToUint32(endIP)
		start := IPv4ToUint32(startIP)
		return end+1 == start
	}

	// IPv6: increment endIP and compare
	next := endIP.Next()
	return next == startIP
}

// SmallestNetwork returns the smaller (more specific) of two overlapping network prefixes.
func SmallestNetwork(a, b netip.Prefix) netip.Prefix {
	// The network with more bits (longer prefix length) is more specific
	if a.Bits() >= b.Bits() {
		return a
	}
	return b
}
