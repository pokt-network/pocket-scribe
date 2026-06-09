package fileplugin

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	natsx "github.com/pokt-network/pocketscribe/internal/nats"
)

// Bootstrap scans dir for block-{H}-meta files, sorts them by height ascending,
// and publishes each file's raw bytes to pokt.block.{H} (ADR-022: one block-level
// message per height). Heights above maxHeight are skipped; maxHeight==0 means no
// cap. Returns the count of messages published.
//
// This is the --bootstrap mode of ps fileplugin: a dumb byte-forwarder that
// republishes captured FilePlugin output to NATS so the block consumer can
// replay history without a live node.
func Bootstrap(ctx context.Context, client *natsx.Client, dir string, maxHeight int64) (int, error) {
	pattern := filepath.Join(dir, "block-*-meta")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, fmt.Errorf("glob %s: %w", pattern, err)
	}

	type entry struct {
		height int64
		path   string
	}
	entries := make([]entry, 0, len(matches))
	for _, p := range matches {
		h, err := parseMetaHeight(filepath.Base(p))
		if err != nil {
			continue // skip non-conforming filenames
		}
		if maxHeight > 0 && h > maxHeight {
			continue
		}
		entries = append(entries, entry{height: h, path: p})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].height < entries[j].height })

	js := client.JetStream()
	n := 0
	for _, e := range entries {
		raw, err := os.ReadFile(e.path)
		if err != nil {
			return n, fmt.Errorf("read %s: %w", e.path, err)
		}
		subj := natsx.BlockSubject(e.height)
		msgID := natsx.MsgID(subj, e.height, 0)
		if _, err := js.Publish(ctx, subj, raw, jetstream.WithMsgID(msgID)); err != nil {
			return n, fmt.Errorf("publish height %d: %w", e.height, err)
		}
		n++
	}
	return n, nil
}

// parseMetaHeight extracts the height from a filename of the form block-{H}-meta.
func parseMetaHeight(base string) (int64, error) {
	if !strings.HasPrefix(base, "block-") || !strings.HasSuffix(base, "-meta") {
		return 0, fmt.Errorf("not a block-meta filename: %q", base)
	}
	inner := base[len("block-") : len(base)-len("-meta")]
	h, err := strconv.ParseInt(inner, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height from %q: %w", base, err)
	}
	return h, nil
}
