package mmdb

import (
	"fmt"
)

// NormalizeSegments normalizes path segments by converting int64 to int and
// validating types. Use this once during initialization and cache the result
// to avoid repeated allocations.
func NormalizeSegments(path []any) ([]any, error) {
	return normalizeSegments(path)
}

func normalizeSegments(path []any) ([]any, error) {
	// Empty path is allowed - it means "decode entire record"
	if len(path) == 0 {
		return []any{}, nil
	}

	segments := make([]any, len(path))
	for i, seg := range path {
		switch v := seg.(type) {
		case string:
			segments[i] = v
		case int:
			segments[i] = v
		case int64:
			if v > int64(int(^uint(0)>>1)) || v < int64(minInt()) {
				return nil, fmt.Errorf("path index %d out of range", v)
			}
			segments[i] = int(v)
		default:
			return nil, fmt.Errorf("unsupported path segment type %T", seg)
		}
	}

	return segments, nil
}

func minInt() int {
	n := ^uint(0) >> 1
	return -int(n) - 1
}
