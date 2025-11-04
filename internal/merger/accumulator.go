package merger

import (
	"fmt"
	"net/netip"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"go4.org/netipx"

	"github.com/maxmind/mmdbconvert/internal/network"
)

// AccumulatedRange represents a continuous IP range with associated data.
type AccumulatedRange struct {
	StartIP netip.Addr
	EndIP   netip.Addr
	Data    mmdbtype.Map // column_name -> value
}

// RowWriter defines the interface for writing output rows.
type RowWriter interface {
	// WriteRow writes a single row with network prefix and column data.
	WriteRow(prefix netip.Prefix, data mmdbtype.Map) error
}

// RangeRowWriter can accept full start/end ranges instead of prefixes.
type RangeRowWriter interface {
	WriteRange(start, end netip.Addr, data mmdbtype.Map) error
}

// Accumulator accumulates adjacent networks with identical data and flushes
// them as CIDRs when data changes. This enables O(1) memory usage.
type Accumulator struct {
	current          *AccumulatedRange
	writer           RowWriter
	includeEmptyRows bool
}

// NewAccumulator creates a new streaming accumulator.
func NewAccumulator(writer RowWriter, includeEmptyRows bool) *Accumulator {
	return &Accumulator{
		writer:           writer,
		includeEmptyRows: includeEmptyRows,
	}
}

// Process handles an incoming network with its data. If the network is adjacent
// to the current accumulated range and has identical data, it extends the range.
// Otherwise, it flushes the current range and starts a new accumulation.
func (a *Accumulator) Process(prefix netip.Prefix, data mmdbtype.Map) error {
	// Skip rows with no data if includeEmptyRows is false (default)
	if !a.includeEmptyRows && len(data) == 0 {
		return nil
	}

	addr := prefix.Addr()
	endIP := netipx.PrefixLastIP(prefix)

	// First network
	if a.current == nil {
		a.current = &AccumulatedRange{
			StartIP: addr,
			EndIP:   endIP,
			Data:    data,
		}
		return nil
	}

	// Check if we can extend current accumulation
	canExtend := network.IsAdjacent(a.current.EndIP, addr) && dataEquals(a.current.Data, data)

	if canExtend {
		// Extend the current range
		a.current.EndIP = endIP
		return nil
	}

	// Data changed or not adjacent - flush current range
	if err := a.Flush(); err != nil {
		return err
	}

	// Start new accumulation
	a.current = &AccumulatedRange{
		StartIP: addr,
		EndIP:   endIP,
		Data:    data,
	}

	return nil
}

// Flush writes the current accumulated range as one or more CIDR rows.
// An accumulated range may produce multiple CIDRs if it doesn't align perfectly.
func (a *Accumulator) Flush() error {
	if a.current == nil {
		return nil
	}

	if rangeWriter, ok := a.writer.(RangeRowWriter); ok {
		if err := rangeWriter.WriteRange(a.current.StartIP, a.current.EndIP, a.current.Data); err != nil {
			return fmt.Errorf(
				"writing range %s-%s: %w",
				a.current.StartIP,
				a.current.EndIP,
				err,
			)
		}
		a.current = nil
		return nil
	}

	// Convert the IP range to valid CIDRs
	cidrs := netipx.IPRangeFrom(a.current.StartIP, a.current.EndIP).Prefixes()

	// Write each CIDR as a separate row
	for _, cidr := range cidrs {
		if err := a.writer.WriteRow(cidr, a.current.Data); err != nil {
			return fmt.Errorf("writing row for %s: %w", cidr, err)
		}
	}

	// Clear current accumulation
	a.current = nil
	return nil
}

// dataEquals performs equality check for data maps using mmdbtype.Map.Equal.
func dataEquals(a, b mmdbtype.Map) bool {
	return a.Equal(b)
}
