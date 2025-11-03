package mmdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeSegments(t *testing.T) {
	tests := []struct {
		name     string
		path     []any
		expected []any
		wantErr  bool
	}{
		{
			name:    "empty path",
			path:    []any{},
			wantErr: true,
		},
		{
			name:     "simple path",
			path:     []any{"country", "iso_code"},
			expected: []any{"country", "iso_code"},
		},
		{
			name:     "array path with index",
			path:     []any{"subdivisions", int64(0), "iso_code"},
			expected: []any{"subdivisions", 0, "iso_code"},
		},
		{
			name:     "nested path",
			path:     []any{"location", "latitude"},
			expected: []any{"location", "latitude"},
		},
		{
			name:     "negative array index",
			path:     []any{"subdivisions", int64(-1), "names", "en"},
			expected: []any{"subdivisions", -1, "names", "en"},
		},
		{
			name:     "multiple array indices",
			path:     []any{"path", int64(0), "sub", int64(1), "value"},
			expected: []any{"path", 0, "sub", 1, "value"},
		},
		{
			name:    "invalid type",
			path:    []any{1.23},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := normalizeSegments(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
