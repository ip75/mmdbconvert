package mmdb

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDataDir  = "../../testdata/MaxMind-DB/test-data"
	cityTestDB   = testDataDir + "/GeoIP2-City-Test.mmdb"
	anonTestDB   = testDataDir + "/GeoIP2-Anonymous-IP-Test.mmdb"
	ipv4TestDB   = testDataDir + "/MaxMind-DB-test-ipv4-24.mmdb"
	brokenTestDB = testDataDir + "/GeoIP2-City-Test-Broken-Double-Format.mmdb"
)

func TestOpen(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "valid MMDB file",
			path:    cityTestDB,
			wantErr: false,
		},
		{
			name:    "non-existent file",
			path:    "/nonexistent/path/database.mmdb",
			wantErr: true,
		},
		{
			name:    "empty path",
			path:    "",
			wantErr: true,
		},
		{
			name:    "invalid MMDB file",
			path:    testDataDir + "/README.md",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := Open(tt.path)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, reader)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, reader)
			assert.Equal(t, tt.path, reader.Path())
			defer reader.Close()
		})
	}
}

func TestReader_Close(t *testing.T) {
	reader, err := Open(cityTestDB)
	require.NoError(t, err)

	err = reader.Close()
	assert.NoError(t, err)
}

func TestReader_Metadata(t *testing.T) {
	reader, err := Open(cityTestDB)
	require.NoError(t, err)
	defer reader.Close()

	metadata := reader.Metadata()
	assert.NotEmpty(t, metadata.DatabaseType)
	assert.Positive(t, metadata.NodeCount)
	assert.Positive(t, metadata.RecordSize)
}

func TestOpenDatabases(t *testing.T) {
	tests := []struct {
		name      string
		databases map[string]string
		wantErr   bool
	}{
		{
			name:      "empty databases map",
			databases: map[string]string{},
			wantErr:   false,
		},
		{
			name: "single valid database",
			databases: map[string]string{
				"city": cityTestDB,
			},
			wantErr: false,
		},
		{
			name: "multiple valid databases",
			databases: map[string]string{
				"city": cityTestDB,
				"anon": anonTestDB,
			},
			wantErr: false,
		},
		{
			name: "single non-existent database",
			databases: map[string]string{
				"test": "/nonexistent/database.mmdb",
			},
			wantErr: true,
		},
		{
			name: "multiple databases with one invalid",
			databases: map[string]string{
				"valid":   cityTestDB,
				"invalid": "/nonexistent/invalid.mmdb",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readers, err := OpenDatabases(tt.databases)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, readers)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, readers)
			defer readers.Close()
		})
	}
}

func TestReaders_Get(t *testing.T) {
	databases := map[string]string{
		"city": cityTestDB,
		"anon": anonTestDB,
	}

	readers, err := OpenDatabases(databases)
	require.NoError(t, err)
	defer readers.Close()

	t.Run("get existing database", func(t *testing.T) {
		reader, ok := readers.Get("city")
		assert.True(t, ok)
		assert.NotNil(t, reader)
		assert.Equal(t, cityTestDB, reader.Path())
	})

	t.Run("get non-existent database", func(t *testing.T) {
		reader, ok := readers.Get("nonexistent")
		assert.False(t, ok)
		assert.Nil(t, reader)
	})
}

func TestReaders_Close(t *testing.T) {
	t.Run("close empty readers", func(t *testing.T) {
		readers := &Readers{
			readers: map[string]*Reader{},
		}
		err := readers.Close()
		assert.NoError(t, err)
	})

	t.Run("close multiple readers", func(t *testing.T) {
		databases := map[string]string{
			"city": cityTestDB,
			"anon": anonTestDB,
		}

		readers, err := OpenDatabases(databases)
		require.NoError(t, err)

		err = readers.Close()
		assert.NoError(t, err)
	})
}

func TestReader_Networks(t *testing.T) {
	reader, err := Open(ipv4TestDB)
	require.NoError(t, err)
	defer reader.Close()

	count := 0
	for result := range reader.Networks() {
		assert.True(t, result.Found())
		prefix := result.Prefix()
		assert.True(t, prefix.IsValid())
		count++
	}
	assert.Positive(t, count, "should have at least one network")
}

func TestReader_NetworksWithin(t *testing.T) {
	reader, err := Open(cityTestDB)
	require.NoError(t, err)
	defer reader.Close()

	// Test with a prefix that should contain some networks
	searchPrefix := netip.MustParsePrefix("81.2.69.0/24")

	count := 0
	for result := range reader.NetworksWithin(searchPrefix) {
		assert.True(t, result.Found())
		network := result.Prefix()
		assert.True(t, network.IsValid())
		// Verify network is within the prefix
		assert.True(t, searchPrefix.Overlaps(network), "network should be within prefix")
		count++
	}
	assert.Positive(t, count, "should have at least one network in prefix")
}

func TestReader_Lookup(t *testing.T) {
	reader, err := Open(cityTestDB)
	require.NoError(t, err)
	defer reader.Close()

	t.Run("lookup existing IP", func(t *testing.T) {
		// This IP should exist in the City test database
		ip := netip.MustParseAddr("81.2.69.142")
		result := reader.Lookup(ip)
		assert.True(t, result.Found())
	})

	t.Run("lookup non-existent IP", func(t *testing.T) {
		// This IP should not exist in the database
		ip := netip.MustParseAddr("10.0.0.1")
		result := reader.Lookup(ip)
		assert.False(t, result.Found())
	})
}
