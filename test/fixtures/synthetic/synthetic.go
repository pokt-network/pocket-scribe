// Package synthetic generates marker-byte block fixtures for Phase B
// orchestration tests — no real proto. The on-disk layout mirrors FilePlugin
// output (block-<H>-meta / block-<H>-data) so Phase D can reuse the path.
package synthetic

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

var (
	dataMarker = []byte("PSCRIBE-DATA\x00")
	metaMarker = []byte("PSCRIBE-META\x00")
)

// MarkerData returns the deterministic data payload for height h.
func MarkerData(h int64) []byte { return marker(dataMarker, h) }

// MarkerMeta returns the deterministic meta payload for height h.
func MarkerMeta(h int64) []byte { return marker(metaMarker, h) }

func marker(prefix []byte, h int64) []byte {
	b := make([]byte, len(prefix)+8)
	copy(b, prefix)
	binary.BigEndian.PutUint64(b[len(prefix):], uint64(h))
	return b
}

func fileName(h int64, suffix string) string {
	return fmt.Sprintf("block-%d-%s", h, suffix)
}

// Generate writes block-<H>-meta and block-<H>-data marker files into dir for
// heights lo..hi inclusive.
func Generate(dir string, lo, hi int64) error {
	for h := lo; h <= hi; h++ {
		if err := os.WriteFile(filepath.Join(dir, fileName(h, "meta")), MarkerMeta(h), 0o644); err != nil {
			return fmt.Errorf("write meta %d: %w", h, err)
		}
		if err := os.WriteFile(filepath.Join(dir, fileName(h, "data")), MarkerData(h), 0o644); err != nil {
			return fmt.Errorf("write data %d: %w", h, err)
		}
	}
	return nil
}
