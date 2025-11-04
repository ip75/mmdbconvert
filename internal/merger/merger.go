// Package merger implements streaming network merge algorithm using nested iteration.
//
// The merger processes networks from multiple MMDB databases, resolving overlaps
// by selecting the smallest network at each point. Adjacent networks with identical
// data are automatically merged for compact output. The streaming accumulator ensures
// O(1) memory usage regardless of database size.
package merger

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/oschwald/maxminddb-golang/v2"

	"github.com/maxmind/mmdbconvert/internal/config"
	"github.com/maxmind/mmdbconvert/internal/mmdb"
	"github.com/maxmind/mmdbconvert/internal/network"
)

// columnExtractor caches the reader and path segments for a column to avoid
// per-row lookups and allocations.
type columnExtractor struct {
	reader   *mmdb.Reader    // Pre-resolved reader for this column
	path     []any           // Cached path segments (avoids per-row slice allocation)
	name     mmdbtype.String // Column name for error messages and map key
	database string          // Database name for error messages
	dbIndex  int             // Index in readersList for O(1) Result lookup
}

// Merger handles merging multiple MMDB databases into a single output stream.
type Merger struct {
	readers     *mmdb.Readers
	config      *config.Config
	acc         *Accumulator
	readersList []*mmdb.Reader    // Ordered list of readers for iteration
	dbNamesList []string          // Corresponding database names
	extractors  []columnExtractor // Pre-built extractors for each column
	unmarshaler *mmdbtype.Unmarshaler
}

// NewMerger creates a new merger instance.
// Returns an error if database readers are missing or path normalization fails.
func NewMerger(readers *mmdb.Readers, cfg *config.Config, writer RowWriter) (*Merger, error) {
	includeEmptyRows := false
	if cfg.Output.IncludeEmptyRows != nil {
		includeEmptyRows = *cfg.Output.IncludeEmptyRows
	}

	// Create Merger instance early so we can call methods on it
	m := &Merger{
		readers:     readers,
		config:      cfg,
		acc:         NewAccumulator(writer, includeEmptyRows),
		unmarshaler: mmdbtype.NewUnmarshaler(),
	}

	// Build ordered list of unique database names
	// This determines the order for readersList and dbIndex values
	dbNamesList := m.getUniqueDatabaseNames()
	if len(dbNamesList) == 0 {
		return nil, errors.New("no databases configured")
	}
	m.dbNamesList = dbNamesList

	// Build readersList in the same order
	readersList := make([]*mmdb.Reader, 0, len(dbNamesList))
	for _, dbName := range dbNamesList {
		reader, ok := readers.Get(dbName)
		if !ok {
			return nil, fmt.Errorf("database '%s' not found", dbName)
		}
		readersList = append(readersList, reader)
	}
	m.readersList = readersList

	// Validate IP versions before building extractors
	if err := validateIPVersions(readersList, dbNamesList); err != nil {
		return nil, err
	}

	// Pre-build column extractors with dbIndex values
	extractors := make([]columnExtractor, len(cfg.Columns))
	for i, column := range cfg.Columns {
		reader, ok := readers.Get(column.Database)
		if !ok {
			return nil, fmt.Errorf(
				"database '%s' not found for column '%s'",
				column.Database,
				column.Name,
			)
		}

		// Normalize path segments once to avoid per-row normalization allocation
		// This converts int64 to int and validates segment types
		pathSegments, err := mmdb.NormalizeSegments(column.Path)
		if err != nil {
			return nil, fmt.Errorf(
				"normalizing path for column '%s': %w",
				column.Name,
				err,
			)
		}

		// Find database index for O(1) lookup in extractAndProcess
		dbIdx := -1
		for j, name := range dbNamesList {
			if name == column.Database {
				dbIdx = j
				break
			}
		}

		extractors[i] = columnExtractor{
			reader:   reader,
			path:     pathSegments,
			name:     column.Name,
			database: column.Database,
			dbIndex:  dbIdx,
		}
	}
	m.extractors = extractors

	return m, nil
}

// Merge performs the streaming merge of all databases.
// It uses nested NetworksWithin iteration to find the smallest overlapping
// networks across all databases, then extracts data and streams to accumulator.
func (m *Merger) Merge() error {
	// readersList and dbNamesList are already built in NewMerger()
	firstReader := m.readersList[0]

	// Iterate all networks in the first database
	for result := range firstReader.Networks(maxminddb.IncludeNetworksWithoutData()) {
		if err := result.Err(); err != nil {
			return fmt.Errorf("iterating first database: %w", err)
		}

		prefix := result.Prefix()

		// If there's only one database, extract and process directly
		if len(m.readersList) == 1 {
			if err := m.extractAndProcess([]maxminddb.Result{result}, prefix); err != nil {
				return err
			}
			continue
		}

		// Process this network through remaining databases starting at index 1
		if err := m.processNetwork([]maxminddb.Result{result}, prefix, 1); err != nil {
			return err
		}
	}

	// Flush any remaining accumulated data
	if err := m.acc.Flush(); err != nil {
		return fmt.Errorf("flushing accumulator: %w", err)
	}

	return nil
}

// processNetwork recursively processes a network through remaining databases.
// It accumulates Results from each database and tracks the effective prefix.
//
// Invariants:
// - results[i] corresponds to readersList[i] for i < dbIndex.
// - effectivePrefix is the smallest network across all databases so far.
// - With IncludeNetworksWithoutData, we always get at least one Result per database.
func (m *Merger) processNetwork(
	results []maxminddb.Result,
	effectivePrefix netip.Prefix,
	dbIndex int,
) error {
	// Base case: processed all databases - extract data
	if dbIndex >= len(m.readersList) {
		return m.extractAndProcess(results, effectivePrefix)
	}

	currentReader := m.readersList[dbIndex]

	// Iterate networks within effectivePrefix in this database
	// With IncludeNetworksWithoutData, this ALWAYS yields at least one Result
	for result := range currentReader.NetworksWithin(effectivePrefix, maxminddb.IncludeNetworksWithoutData()) {
		if err := result.Err(); err != nil {
			return fmt.Errorf("iterating database within %s: %w", effectivePrefix, err)
		}

		nextNetwork := result.Prefix()

		// Determine smallest (most specific) network
		smallest := network.SmallestNetwork(effectivePrefix, nextNetwork)

		// CRITICAL: Always append Result from THIS database (readersList[dbIndex])
		// This maintains results[i] ↔ readersList[i] correspondence
		// Result may have Found() == false if database has no data (notFound offset)
		// Use slices.Concat to guarantee new allocation and avoid shared arrays
		newResults := slices.Concat(results, []maxminddb.Result{result})

		// Recurse with the smallest prefix
		// NOTE: smallest may be smaller than result.Prefix() - that's OK!
		// The result contains data for a broader network that covers smallest.
		if err := m.processNetwork(newResults, smallest, dbIndex+1); err != nil {
			return err
		}
	}

	return nil
}

// extractAndProcess extracts data for all columns using precomputed Results,
// then feeds the result to the accumulator.
//
// This function performs NO database lookups - all Results come from the slice.
// Invariants:
// - results[i] corresponds to readersList[i].
// - effectivePrefix is the actual network being processed (may be smaller than result.Prefix()).
func (m *Merger) extractAndProcess(
	results []maxminddb.Result,
	effectivePrefix netip.Prefix,
) error {
	// Pre-allocate map capacity to avoid dynamic growth
	data := make(mmdbtype.Map, len(m.extractors))

	// Extract values for all columns using Results
	for _, extractor := range m.extractors {
		// Check if reader was resolved during initialization
		if extractor.reader == nil {
			return fmt.Errorf(
				"database '%s' not found for column '%s'",
				extractor.database,
				extractor.name,
			)
		}

		// Get Result for this database using cached index
		if extractor.dbIndex < 0 || extractor.dbIndex >= len(results) {
			// Database index out of bounds - skip column
			continue
		}

		result := results[extractor.dbIndex]

		// Extract directly from Result - NO LOOKUP!
		// DecodePath handles notFound Results internally (checks Found(), returns early)
		//
		// IMPORTANT: effectivePrefix may be smaller than result.Prefix()
		// This is valid - the result contains data for a broader network,
		// and DecodePath correctly handles querying within that network.
		if err := result.DecodePath(m.unmarshaler, extractor.path...); err != nil {
			return fmt.Errorf(
				"decoding path for column '%s': %w",
				extractor.name,
				err,
			)
		}

		value := m.unmarshaler.Result()
		m.unmarshaler.Clear()

		// Only add non-nil values to reduce allocations and simplify empty detection
		if value != nil {
			data[extractor.name] = value
		}
	}

	// Use the effectivePrefix parameter - NOT derived from results!
	return m.acc.Process(effectivePrefix, data)
}

// getUniqueDatabaseNames returns the list of unique database names used in columns.
func (m *Merger) getUniqueDatabaseNames() []string {
	seen := map[string]bool{}
	var names []string

	for _, column := range m.config.Columns {
		if !seen[column.Database] {
			seen[column.Database] = true
			names = append(names, column.Database)
		}
	}

	return names
}

func validateIPVersions(readers []*mmdb.Reader, names []string) error {
	var (
		ipv4Only     []string
		ipv6Capable  []string
		unsupportedV []string
	)

	for idx, reader := range readers {
		version := reader.Metadata().IPVersion
		switch version {
		case 4:
			ipv4Only = append(ipv4Only, names[idx])
		case 6:
			ipv6Capable = append(ipv6Capable, names[idx])
		default:
			unsupportedV = append(
				unsupportedV,
				fmt.Sprintf("%s (ip_version=%d)", names[idx], version),
			)
		}
	}

	if len(unsupportedV) > 0 {
		return fmt.Errorf(
			"unsupported ip_version values reported: %s",
			strings.Join(unsupportedV, ", "),
		)
	}

	if len(ipv4Only) > 0 && len(ipv6Capable) > 0 {
		return fmt.Errorf(
			"configured databases mix IPv4-only (%s) and IPv6-capable (%s) files; run separate conversions per IP version or supply homogeneous databases",
			strings.Join(ipv4Only, ", "),
			strings.Join(ipv6Capable, ", "),
		)
	}

	return nil
}
