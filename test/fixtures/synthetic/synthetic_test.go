package synthetic

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestMarkerDataDeterministicAndHeightTagged(t *testing.T) {
	a := MarkerData(635505)
	b := MarkerData(635505)
	if !bytes.Equal(a, b) {
		t.Fatal("MarkerData not deterministic")
	}
	if bytes.Equal(MarkerData(1), MarkerData(2)) {
		t.Fatal("MarkerData should differ by height")
	}
	if !bytes.HasPrefix(a, []byte("PSCRIBE-DATA")) {
		t.Fatalf("MarkerData missing marker prefix: %q", a)
	}
}

func TestGenerateWritesFiles(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, 1, 3); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	for h := 1; h <= 3; h++ {
		for _, suffix := range []string{"meta", "data"} {
			p := filepath.Join(dir, // block-<h>-<suffix>
				fileName(int64(h), suffix))
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("expected file %s: %v", p, err)
			}
		}
	}
}
