package router

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Supplier-closure seed types. Append the three pocket.migration claim types
// when that decode scope lands (docs/research/supplier-shape-breaks.md §6).
var shapeGuardSeeds = []string{
	"pocket.supplier.MsgStakeSupplier",
	"pocket.supplier.MsgUnstakeSupplier",
	"pocket.supplier.MsgStakeSupplierResponse",
	"pocket.supplier.MsgUnstakeSupplierResponse",
	"pocket.supplier.EventSupplierStaked",
	"pocket.supplier.EventSupplierUnbondingBegin",
	"pocket.supplier.EventSupplierUnbondingEnd",
	"pocket.supplier.EventSupplierUnbondingCanceled",
	"pocket.supplier.EventSupplierServiceConfigActivated",
	"pocket.tokenomics.EventSupplierSlashed",
	"pocket.shared.Supplier",
}

type shapeField struct {
	Tag      int    `json:"tag"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Repeated bool   `json:"repeated"`
}

type shapeMessage struct {
	Fields []shapeField `json:"fields"`
}

type shapeSnapshot struct {
	Version  string                  `json:"version"`
	Messages map[string]shapeMessage `json:"messages"`
}

// loadSnapshots globs the .shapes dir and returns snapshots sorted numerically
// by patch (v0_1_2 must sort before v0_1_10 — NOT lexicographic).
func loadSnapshots(t *testing.T) []shapeSnapshot {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "docs", "research", ".shapes", "v0_1_*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no shape snapshots found: %v", err)
	}
	patch := func(p string) int {
		base := strings.TrimSuffix(filepath.Base(p), ".json") // v0_1_N
		n, err := strconv.Atoi(strings.TrimPrefix(base, "v0_1_"))
		if err != nil {
			t.Fatalf("bad snapshot filename %q: %v", p, err)
		}
		return n
	}
	sort.Slice(paths, func(i, j int) bool { return patch(paths[i]) < patch(paths[j]) })
	snaps := make([]shapeSnapshot, 0, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p) //nolint:gosec // paths come from filepath.Glob over a known local research directory; no user input
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		var s shapeSnapshot
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		// The FILENAME governs the version string; the snapshot's own version
		// field may use either "v0.1.8" or "v0_1_8" spelling and is ignored.
		s.Version = strings.TrimSuffix(filepath.Base(p), ".json")
		snaps = append(snaps, s)
	}
	return snaps
}

// closure BFS-expands the seed set: a field's type resolves to a message key by
// trying (i) exact, (ii) <package-of-container>.<type>, (iii) "pocket."+type.
// Unresolvable types (scalars, enums, cosmos imports) are skipped.
func closure(s shapeSnapshot) map[string]shapeMessage {
	out := map[string]shapeMessage{}
	queue := append([]string(nil), shapeGuardSeeds...)
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if _, done := out[name]; done {
			continue
		}
		msg, ok := s.Messages[name]
		if !ok {
			continue
		}
		out[name] = msg
		pkg := name[:strings.LastIndex(name, ".")]
		for _, f := range msg.Fields {
			for _, cand := range []string{f.Type, pkg + "." + f.Type, "pocket." + f.Type} {
				if _, ok := s.Messages[cand]; ok {
					queue = append(queue, cand)
					break
				}
			}
		}
	}
	return out
}

// canon normalizes a message to its tag-sorted (tag,name,type,repeated) tuples.
func canon(m shapeMessage) string {
	fs := append([]shapeField(nil), m.Fields...)
	sort.Slice(fs, func(i, j int) bool { return fs[i].Tag < fs[j].Tag })
	var b strings.Builder
	for _, f := range fs {
		fmt.Fprintf(&b, "%d|%s|%s|%v;", f.Tag, f.Name, f.Type, f.Repeated)
	}
	return b.String()
}

// TestSupplierShapeGuard: any difference in the canonical form of any
// transitively-reachable closure message between consecutive snapshots marks
// the LATER version as a break version; every break version (plus the oldest
// snapshot) must be in DefaultRegistry(). Deliberately stricter than
// wire-breaking: an additive field is silently DROPPED under earlier-decoder
// fallback — data loss for an indexer. Known blind spots (documented, not
// asserted): enum values and `reserved` ranges are absent from .shapes.
func TestSupplierShapeGuard(t *testing.T) {
	snaps := loadSnapshots(t)
	reg := DefaultRegistry()
	required := map[string][]string{snaps[0].Version: {"(oldest snapshot baseline)"}}
	prev := closure(snaps[0])
	prevV := snaps[0].Version
	for _, s := range snaps[1:] {
		cur := closure(s)
		var changed []string
		seen := map[string]bool{}
		for name := range prev {
			seen[name] = true
			a, inA := prev[name]
			b, inB := cur[name]
			if inA != inB || canon(a) != canon(b) {
				changed = append(changed, name)
			}
		}
		for name := range cur {
			if !seen[name] {
				changed = append(changed, name) // type entered the closure
			}
		}
		if len(changed) > 0 {
			sort.Strings(changed)
			required[s.Version] = changed
		}
		prev, prevV = cur, s.Version
	}
	_ = prevV
	var missing []string
	for v, types := range required {
		if _, ok := reg[v]; !ok {
			missing = append(missing, fmt.Sprintf("%s (changed: %s)", v, strings.Join(types, ", ")))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("supplier shape-break versions missing from router.DefaultRegistry() — "+
			"register a decoder package for each (ADR-008) or supplier rows will be "+
			"silently mis-decoded under lenient fallback:\n  %s", strings.Join(missing, "\n  "))
	}
}
