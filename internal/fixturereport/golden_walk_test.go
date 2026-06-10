package fixturereport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestGoldenWalkAllFixtures re-derives every fixture's expected.json through
// the live decode pipeline. Adding a fixture triplet automatically enrolls it
// — this is the "N versions × categories green" enforcement for Phase F.
//
// TODO(phase-f-pending): v0.1.30 / v0.1.31 / v0.1.33 have no archived
// FilePlugin data yet (archeology still running on multi-1). When their
// tarballs land in the bucket, run the curate-version-fixtures skill —
// the new triplets enroll here automatically. See test/fixtures/README.md.
func TestGoldenWalkAllFixtures(t *testing.T) {
	r := mustRouter(t)
	pattern := "../../test/fixtures/v0_1_*/block-*-expected.json"
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 13 {
		t.Fatalf("found only %d expected.json files (%s) — corpus missing?", len(files), pattern)
	}
	for _, ef := range files {
		name := filepath.Base(filepath.Dir(ef)) + "/" + filepath.Base(ef)
		t.Run(name, func(t *testing.T) {
			var h int64
			if _, err := fmt.Sscanf(filepath.Base(ef), "block-%d-expected.json", &h); err != nil {
				t.Fatalf("unparseable fixture name: %v", err)
			}
			dir := filepath.Dir(ef)
			meta, err := os.ReadFile(fxPath(dir, h, "meta"))
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(fxPath(dir, h, "data"))
			if err != nil {
				t.Fatal(err)
			}
			got, err := Report(r, meta, data)
			if err != nil {
				t.Fatalf("Report: %v", err)
			}
			raw, err := os.ReadFile(ef) //nolint:gosec
			if err != nil {
				t.Fatal(err)
			}
			var want Result
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*got, want) {
				t.Fatalf("mismatch:\ngot  %+v\nwant %+v", *got, want)
			}
		})
	}
}
