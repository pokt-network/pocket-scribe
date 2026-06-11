// Command covgate enforces per-package coverage thresholds from a merged cover
// profile: 100% for internal/decoders/* (excluding gen/), 90% for everything
// else under internal/. Packages with zero statements are skipped. Generated
// trees ("/gen/") and internal/proto are excluded entirely.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

const module = "github.com/pokt-network/pocketscribe/"

type agg struct{ stmts, covered int64 }

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: covgate merged.cov")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1]) //nolint:gosec // CLI tool; path from developer
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pkgs := parseProfile(f) // extracted for unit-testability (no os.Exit inside)
	if err := f.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "close:", err)
		os.Exit(1)
	}
	failed := report(pkgs, os.Stdout)
	if failed {
		os.Exit(1)
	}
}

// parseProfile aggregates statement/covered counts per package from a cover
// profile. Each line: "path/file.go:l.c,l.c numStmt count".
func parseProfile(r io.Reader) map[string]*agg {
	pkgs := map[string]*agg{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		fields := strings.Fields(line) // [file:ranges, numStmt, count]
		if len(fields) != 3 {
			continue
		}
		// The file path ends at the last ':' before the ranges (which contain '.' and ',').
		file := fields[0][:strings.LastIndexByte(fields[0], ':')]
		nstmt, err1 := strconv.ParseInt(fields[1], 10, 64)
		cnt, err2 := strconv.ParseInt(fields[2], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		pkg := strings.TrimPrefix(path.Dir(file), module)
		if strings.Contains(pkg, "/gen") || pkg == "internal/proto" || !strings.HasPrefix(pkg, "internal/") {
			continue
		}
		a := pkgs[pkg]
		if a == nil {
			a = &agg{}
			pkgs[pkg] = a
		}
		a.stmts += nstmt
		if cnt > 0 {
			a.covered += nstmt
		}
	}
	return pkgs
}

// report prints the per-package table sorted by name and returns whether any
// package is below its bar.
func report(pkgs map[string]*agg, w io.Writer) bool {
	names := make([]string, 0, len(pkgs))
	for pkg := range pkgs {
		names = append(names, pkg)
	}
	sort.Strings(names)
	failed := false
	for _, pkg := range names {
		a := pkgs[pkg]
		if a.stmts == 0 {
			continue
		}
		pct := 100 * float64(a.covered) / float64(a.stmts)
		want := 90.0
		if strings.HasPrefix(pkg, "internal/decoders/") || pkg == "internal/decoders" {
			want = 100.0
		}
		status := "ok"
		if pct < want {
			status = "FAIL"
			failed = true
		}
		_, _ = fmt.Fprintf(w, "%-50s %6.1f%% (want %.0f%%) %s\n", pkg, pct, want, status)
	}
	return failed
}
