package store

import (
	"context"
	"fmt"
	"strings"
)

// DecoderVersionIDs returns tag → id from decoder_version (seeded by the
// per-version migrations, e.g. 'v0.1.8'→108). Loaded once at consumer startup.
func (s *Store) DecoderVersionIDs(ctx context.Context) (map[string]int16, error) {
	rows, err := s.pool.Query(ctx, `SELECT tag, id FROM decoder_version`)
	if err != nil {
		return nil, fmt.Errorf("load decoder versions: %w", err)
	}
	defer rows.Close()
	out := map[string]int16{}
	for rows.Next() {
		var tag string
		var id int16
		if err := rows.Scan(&tag, &id); err != nil {
			return nil, fmt.Errorf("scan decoder version: %w", err)
		}
		out[tag] = id
	}
	return out, rows.Err()
}

// DecoderTag converts a decoder package version ("v0_1_8") to the
// decoder_version.tag spelling ("v0.1.8").
func DecoderTag(version string) string { return strings.ReplaceAll(version, "_", ".") }
