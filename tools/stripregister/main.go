// stripregister removes gogoproto GLOBAL registry registration from generated
// .pb.go files so that multiple poktroll decoder versions can coexist in one
// binary (gogoproto's proto.RegisterEnum PANICS on duplicate full names; the
// fully-qualified proto names "pocket.shared.RPCType" etc. are identical
// across every vendored poktroll version).
//
// It strips, inside init() functions only:
//   - proto.RegisterType / proto.RegisterEnum / proto.RegisterMapType lines
//     (multi-line init blocks; the block is dropped entirely if it becomes empty)
//   - single-line `func init() { proto.RegisterFile(...) }` declarations
//   - golang_proto.* variants of all of the above (defensive; gocosmos does
//     not emit them without the gogoproto_registration option)
//
// Everything else is left byte-identical. The transform is deterministic and
// idempotent: running it twice produces the same bytes, so it is safe inside
// gen-check.
//
// Usage: go run ./tools/stripregister <gen-dir> [<gen-dir>...]
package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	// `	proto.RegisterType((*EventSupplierStaked)(nil), "pocket.supplier.EventSupplierStaked")`
	reRegisterLine = regexp.MustCompile(`^\t(proto|golang_proto)\.Register(Type|Enum|MapType|File)\(.*\)$`)
	// `func init() { proto.RegisterFile("pocket/supplier/event.proto", fileDescriptor_x) }`
	reInitOneLine = regexp.MustCompile(`^func init\(\) \{ (proto|golang_proto)\.RegisterFile\(.*\) \}$`)
)

func transform(src []byte) []byte {
	lines := strings.Split(string(src), "\n")
	var out []string
	// skipBlank swallows the blank line following a dropped declaration when
	// the previously emitted line is already blank, so the output stays
	// gofmt-clean (gofmt collapses consecutive blank lines).
	skipBlank := func(i int) int {
		if i+1 < len(lines) && lines[i+1] == "" && len(out) > 0 && out[len(out)-1] == "" {
			return i + 1
		}
		return i
	}
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if reInitOneLine.MatchString(line) {
			i = skipBlank(i)
			continue // drop the whole single-line init
		}
		if line == "func init() {" {
			// Buffer the block; filter register lines; drop block if emptied.
			var body []string
			j := i + 1
			for ; j < len(lines) && lines[j] != "}"; j++ {
				if !reRegisterLine.MatchString(lines[j]) {
					body = append(body, lines[j])
				}
			}
			if len(body) > 0 {
				out = append(out, line)
				out = append(out, body...)
				out = append(out, "}")
				i = j // skip past closing brace
			} else {
				i = skipBlank(j) // block dropped entirely
			}
			continue
		}
		out = append(out, line)
	}
	return []byte(strings.Join(out, "\n"))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: stripregister <gen-dir> [<gen-dir>...]")
		os.Exit(2)
	}
	changed := 0
	for _, root := range os.Args[1:] {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error { //nolint:gosec // G703: paths are developer-controlled codegen output dirs
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".pb.go") {
				return err
			}
			src, err := os.ReadFile(path) //nolint:gosec // G304: developer-controlled .pb.go file path from WalkDir
			if err != nil {
				return err
			}
			dst := transform(src)
			if !bytes.Equal(src, dst) {
				if err := os.WriteFile(path, dst, 0o644); err != nil { //nolint:gosec // G306: .pb.go files are world-readable generated code
					return err
				}
				changed++
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "stripregister: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Printf("stripregister: rewrote %d files\n", changed)
}
