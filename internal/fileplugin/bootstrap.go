package fileplugin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
)

// Bootstrap republishes captured FilePlugin output as the ADR-022 fan-out:
// per-tx (pokt.tx.{H}.{i}), per-event (pokt.events.{type}.{H}), per-KV
// (pokt.kv.{store}.{H}) and FINALLY the metadata-only BlockEnvelope on
// pokt.block.{H} — the envelope is published LAST per height (ordering
// contract, ADR-022 amendment): consumers batch on it as the completeness
// fence. The split is STRUCTURAL: cometbft/cosmos containers only — no
// poktroll decode, no router (decision 1).
//
// Event/KV ordinals (used in Nats-Msg-Id and EventInBlock positions) follow
// the deterministic enumeration order of the captured files: block-level
// events first (ResponseFinalizeBlock.events), then per-tx events in tx
// order; KV pairs in data-file order. Returns (heights, messages) published.
//
// Payload caps: payloads above 256 KiB (SoftCapBytes) are logged and counted
// but still published; payloads above 1 MiB (HardCapBytes) are refused at the
// source — the NATS server's default max_payload would reject them anyway, so
// refusing here keeps the failure explicit and leaves the height un-acked.
// fpm may be nil (tests that do not need metric assertions).
func Bootstrap(ctx context.Context, client *natsx.Client, dir string, maxHeight int64, chainID string, fpm *metrics.FilePlugin) (int, int, error) {
	pattern := filepath.Join(dir, "block-*-meta")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, 0, fmt.Errorf("glob %s: %w", pattern, err)
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
	publish := capPublish(func(subj string, data []byte, msgID string) error {
		_, err := js.Publish(ctx, subj, data, jetstream.WithMsgID(msgID))
		return err
	}, slog.Default(), fpm)
	heights, total := 0, 0
	for _, e := range entries {
		n, err := fanOutHeight(ctx, publish, e.height, e.path, chainID)
		if err != nil {
			return heights, total, fmt.Errorf("height %d: %w", e.height, err)
		}
		heights++
		total += n
	}
	return heights, total, nil
}

// fanOutHeight publishes one height's fan-out + envelope. publish is injected
// for testability.
func fanOutHeight(_ context.Context, publish func(subj string, data []byte, msgID string) error, height int64, metaPath, chainID string) (int, error) {
	metaBytes, err := os.ReadFile(metaPath) //nolint:gosec // path is constructed from validated fixture dir
	if err != nil {
		return 0, fmt.Errorf("read meta: %w", err)
	}
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"
	dataBytes, err := os.ReadFile(dataPath) //nolint:gosec // path is constructed from validated fixture dir
	if err != nil {
		return 0, fmt.Errorf("read data (FilePlugin always writes both, ADR-027): %w", err)
	}
	records, err := decoders.SplitMeta(metaBytes)
	if err != nil {
		return 0, err
	}
	var req abci.RequestFinalizeBlock
	if err := req.Unmarshal(records[0]); err != nil {
		return 0, fmt.Errorf("RequestFinalizeBlock: %w", err)
	}
	var resp abci.ResponseFinalizeBlock
	if err := resp.Unmarshal(records[1]); err != nil {
		return 0, fmt.Errorf("ResponseFinalizeBlock: %w", err)
	}
	header, err := decoders.DecodeBlockHeader(metaBytes)
	if err != nil {
		return 0, err
	}

	n := 0
	// ── txs ──
	for i, txBytes := range req.Txs {
		var resBytes []byte
		if i < len(resp.TxResults) {
			if resBytes, err = resp.TxResults[i].Marshal(); err != nil {
				return n, fmt.Errorf("tx_result %d: %w", i, err)
			}
		}
		raw, err := (&psv1.TxWithResult{Tx: txBytes, Result: resBytes}).Marshal()
		if err != nil {
			return n, err
		}
		subj := natsx.TxSubject(height, i)
		if err := publish(subj, raw, natsx.MsgID(subj, height, i)); err != nil {
			return n, err
		}
		n++
	}
	// ── events: block-level first, then per-tx (deterministic ordinal) ──
	ordinal := 0
	emit := func(ev abci.Event, txIndex int32, eventIndex int32) error {
		evBytes, err := ev.Marshal()
		if err != nil {
			return err
		}
		raw, err := (&psv1.EventInBlock{Event: evBytes, TxIndex: txIndex, EventIndex: eventIndex}).Marshal()
		if err != nil {
			return err
		}
		subj := natsx.EventSubject(ev.Type, height)
		if err := publish(subj, raw, natsx.MsgID(subj, height, ordinal)); err != nil {
			return err
		}
		ordinal++
		n++
		return nil
	}
	for k, ev := range resp.Events {
		if err := emit(ev, -1, int32(k)); err != nil {
			return n, fmt.Errorf("block event %d: %w", k, err)
		}
	}
	for ti, txr := range resp.TxResults {
		for k, ev := range txr.Events {
			if err := emit(ev, int32(ti), int32(k)); err != nil {
				return n, fmt.Errorf("tx %d event %d: %w", ti, k, err)
			}
		}
	}
	eventCount := ordinal
	// ── kv: raw StoreKVPair records in data-file order ──
	kvCount := 0
	rest := dataBytes
	for len(rest) > 0 {
		payload, consumed, err := decoders.ReadDelimited(rest)
		if err != nil {
			return n, fmt.Errorf("data record %d: %w", kvCount, err)
		}
		storeKey, err := decoders.StoreKeyOf(payload)
		if err != nil {
			return n, fmt.Errorf("data record %d: %w", kvCount, err)
		}
		subj := natsx.KVSubject(storeKey, height)
		if err := publish(subj, payload, natsx.MsgID(subj, height, kvCount)); err != nil {
			return n, err
		}
		kvCount++
		n++
		rest = rest[consumed:]
	}
	// ── envelope LAST (the fence) ──
	env := &psv1.BlockEnvelope{
		Height: height, TimeUnixNano: header.Time.UnixNano(),
		Hash: header.Hash, ProposerAddress: header.ProposerAddress, ChainId: chainID,
		TxCount: int32(len(req.Txs)), EventCount: int32(eventCount), KvCount: int32(kvCount),
		PublishedMsgCount: int32(n),
	}
	raw, err := env.Marshal()
	if err != nil {
		return n, err
	}
	subj := natsx.BlockSubject(height)
	if err := publish(subj, raw, natsx.MsgID(subj, height, 0)); err != nil {
		return n, err
	}
	return n + 1, nil
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
