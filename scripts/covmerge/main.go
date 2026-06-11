// Command covmerge merges Go cover profiles (text format) by summing counts
// per block. Usage: covmerge out.cov in1.cov in2.cov [...]
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: covmerge OUT IN...")
		os.Exit(2)
	}

	order, counts, mergeErr := mergeProfiles(os.Args[2:])
	if mergeErr != nil {
		fmt.Fprintln(os.Stderr, mergeErr)
		os.Exit(1)
	}

	out, err := os.Create(os.Args[1]) //nolint:gosec // CLI tool; output path from developer
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	_, _ = fmt.Fprintln(out, "mode: atomic")
	for _, k := range order {
		_, _ = fmt.Fprintf(out, "%s %d\n", k, counts[k])
	}
	if err := out.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "close output:", err)
		os.Exit(1)
	}
}

// mergeProfiles reads the given profile files and returns an ordered slice of
// keys and a map of key → summed count. Exported for unit-testing.
func mergeProfiles(paths []string) ([]string, map[string]int64, error) {
	counts := map[string]int64{}
	order := []string{}
	for _, in := range paths {
		f, err := os.Open(in) //nolint:gosec // CLI tool; paths from developer
		if err != nil {
			return nil, nil, err
		}
		scanErr := mergeReader(f, counts, &order)
		_ = f.Close()
		if scanErr != nil {
			return nil, nil, scanErr
		}
	}
	return order, counts, nil
}

// mergeReader reads one cover-profile stream into counts/order.
func mergeReader(r io.Reader, counts map[string]int64, order *[]string) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "mode:") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		key, cnt := line[:i], line[i+1:]
		var c int64
		_, _ = fmt.Sscanf(cnt, "%d", &c) // malformed count → 0 is safe
		if _, seen := counts[key]; !seen {
			*order = append(*order, key)
		}
		counts[key] += c
	}
	return sc.Err()
}
