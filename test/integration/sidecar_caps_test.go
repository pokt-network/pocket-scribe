//go:build integration

// sidecar_caps_test.go — phase G: large-block payload caps through the
// sidecar→NATS→consumer pipeline (integration-level).
//
// Test 29a (soft cap): a synthetic block at height H1 contains one tx whose
// serialised TxWithResult payload exceeds the 256 KiB soft cap but stays
// below the 1 MiB hard cap.  Bootstrap publishes the height; the block
// consumer processes it; the oversize_soft metric fires once; the consumer
// advances past H1.
//
// Test 29b (hard cap): a synthetic block at height H2 contains one tx whose
// serialised TxWithResult payload exceeds the 1 MiB hard cap.  Bootstrap
// refuses to publish the tx; the height is aborted (no envelope published);
// the oversize_refused metric fires once; the consumer cursor does NOT
// advance to H2.
//
// Fixture format (mirrors internal/fileplugin/bootstrap_test.go):
//
//	block-{H}-meta  — uvarint-framed: [RequestFinalizeBlock, ResponseFinalizeBlock, 0x00]
//	block-{H}-data  — empty (no KV pairs needed for these tests)
//
// The helpers below are duplicated from the internal/fileplugin package
// (unexported) as required by the task spec.  Do NOT export the originals.
package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/pokt-network/pocketscribe/internal/fileplugin"
	"github.com/pokt-network/pocketscribe/internal/metrics"
)

// ─── duplicated length-delimited helpers ─────────────────────────────────────
// These ~30 lines mirror internal/fileplugin/bootstrap_test.go (unexported
// helpers). They are intentionally local here per the task spec.

// sidecarFrameDelimited encodes payload with a uvarint length prefix, matching
// the Cosmos SDK FilePlugin framing used by sidecar's meta files.
func sidecarFrameDelimited(payload []byte) []byte {
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(len(payload)))
	out := make([]byte, 0, n+len(payload))
	out = append(out, prefix[:n]...)
	out = append(out, payload...)
	return out
}

// sidecarBuildMeta constructs a 3-record meta payload:
//
//	record[0] = marshalled RequestFinalizeBlock (contains txs + block time + hash)
//	record[1] = marshalled ResponseFinalizeBlock (contains tx_results + events)
//	record[2] = 0x00 (zero-length ResponseCommit)
//
// This is the exact format fanOutHeight expects when parsing block-{H}-meta.
func sidecarBuildMeta(t *testing.T, req *abci.RequestFinalizeBlock, resp *abci.ResponseFinalizeBlock) []byte {
	t.Helper()
	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatalf("sidecarBuildMeta: marshal req: %v", err)
	}
	respBytes, err := resp.Marshal()
	if err != nil {
		t.Fatalf("sidecarBuildMeta: marshal resp: %v", err)
	}
	var buf bytes.Buffer
	buf.Write(sidecarFrameDelimited(reqBytes))
	buf.Write(sidecarFrameDelimited(respBytes))
	buf.WriteByte(0x00) // zero-length ResponseCommit
	return buf.Bytes()
}

// sidecarWriteFixture writes block-{H}-meta and block-{H}-data to dir.
// data may be nil (empty file is written).
func sidecarWriteFixture(t *testing.T, dir string, height int64, metaBytes, data []byte) {
	t.Helper()
	hStr := int64str(height)
	metaPath := filepath.Join(dir, "block-"+hStr+"-meta")
	dataPath := filepath.Join(dir, "block-"+hStr+"-data")
	if err := os.WriteFile(metaPath, metaBytes, 0o644); err != nil {
		t.Fatalf("sidecarWriteFixture h=%d meta: %v", height, err)
	}
	if data == nil {
		data = []byte{}
	}
	if err := os.WriteFile(dataPath, data, 0o644); err != nil {
		t.Fatalf("sidecarWriteFixture h=%d data: %v", height, err)
	}
}

// sidecarRawTxBytes returns a raw tx byte slice of exactly targetBytes.
// fanOutHeight wraps it as TxWithResult{Tx: rawTx} before publishing, adding
// ~2 bytes proto overhead.  For all practical cap targets (256 KiB, 1 MiB)
// the proto overhead is negligible and the published size > targetBytes.
func sidecarRawTxBytes(targetBytes int) []byte {
	return make([]byte, targetBytes)
}

// ─── tests ────────────────────────────────────────────────────────────────────

// TestSidecarCapsPayloadPolicy (spec tests 29a + 29b; ADR-024 amendment Phase G)
// verifies the payload size policy (256 KiB soft / 1 MiB hard) end-to-end
// through the Bootstrap→NATS→block-consumer pipeline.
func TestSidecarCapsPayloadPolicy(t *testing.T) {
	// ── 29a: soft cap (>256 KiB payload still published) ──────────────────────
	t.Run("soft_cap_published", func(t *testing.T) {
		stream := freshStream(t)
		pg.Reset(t)
		ctx := context.Background()

		// A payload of 300 KiB (> SoftCapBytes=256 KiB, < HardCapBytes=1 MiB).
		const softPayloadBytes = 300 << 10

		// H1 = a height that will not conflict with any existing fixture (use 9001).
		const h1 int64 = 9001
		blockTime := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

		// Raw tx bytes placed in RequestFinalizeBlock.Txs.
		// fanOutHeight wraps them: TxWithResult{Tx: rawTx} → marshal → published payload.
		// Proto overhead is ~2 bytes; since rawTx is 300 KiB, published size > SoftCapBytes.
		rawTx := sidecarRawTxBytes(softPayloadBytes)
		if softPayloadBytes <= fileplugin.SoftCapBytes {
			t.Fatalf("setup: softPayloadBytes %d <= SoftCapBytes %d", softPayloadBytes, fileplugin.SoftCapBytes)
		}
		if softPayloadBytes >= fileplugin.HardCapBytes {
			t.Fatalf("setup: softPayloadBytes %d >= HardCapBytes %d", softPayloadBytes, fileplugin.HardCapBytes)
		}
		req1 := &abci.RequestFinalizeBlock{
			Height: h1,
			Time:   blockTime,
			Hash:   make([]byte, 32),
			Txs:    [][]byte{rawTx}, // one large tx
		}
		resp1 := &abci.ResponseFinalizeBlock{
			TxResults: []*abci.ExecTxResult{{Code: 0}},
		}
		dir := t.TempDir()
		sidecarWriteFixture(t, dir, h1, sidecarBuildMeta(t, req1, resp1), nil)

		// Wire a FilePlugin metrics registry we can inspect.
		fpmReg := prometheus.NewRegistry()
		fpm := metrics.NewFilePlugin(fpmReg)

		// Bootstrap the synthetic fixture dir.
		heights, _, err := fileplugin.Bootstrap(ctx, nats.Client, dir, 0, "pocket", fpm)
		if err != nil {
			t.Fatalf("Bootstrap h1=%d: unexpected error: %v", h1, err)
		}
		if heights != 1 {
			t.Errorf("Bootstrap: heights = %d, want 1", heights)
		}

		// Soft cap metric must be 1.
		if got := testutil.ToFloat64(fpm.OversizeSoft); got != 1 {
			t.Errorf("oversize_soft = %.0f, want 1", got)
		}
		// Hard cap must be 0 (payload was NOT refused).
		if got := testutil.ToFloat64(fpm.OversizeRefused); got != 0 {
			t.Errorf("oversize_refused = %.0f, want 0 (soft-cap block still published)", got)
		}

		// Start block consumer and verify it processes H1 (envelope was published).
		blockRH := startBlockRuntime(t, stream, "block")
		waitHasProcessed(t, blockRH.store, "block", h1, 20*time.Second)

		// Block row must exist.
		var count int
		if err := pg.Pool.QueryRow(ctx, `SELECT count(*) FROM block WHERE height=$1`, h1).Scan(&count); err != nil {
			t.Fatalf("count block h=%d: %v", h1, err)
		}
		if count != 1 {
			t.Errorf("block row count for h=%d = %d, want 1", h1, count)
		}
	})

	// ── 29b: hard cap (>1 MiB payload refused, envelope never published) ──────
	t.Run("hard_cap_refused", func(t *testing.T) {
		stream := freshStream(t)
		pg.Reset(t)
		ctx := context.Background()

		// A payload of ~1.1 MiB (> HardCapBytes=1 MiB).
		const hardPayloadBytes = (1 << 20) + (100 << 10) // 1 MiB + 100 KiB

		const h2 int64 = 9002
		blockTime := time.Date(2026, 3, 15, 13, 0, 0, 0, time.UTC)

		// Raw tx bytes placed in RequestFinalizeBlock.Txs.
		// fanOutHeight wraps them: TxWithResult{Tx: rawTx} → marshal → published payload.
		// Proto overhead is ~2 bytes; since rawTx is ~1.1 MiB, published size > HardCapBytes.
		rawTx2 := sidecarRawTxBytes(hardPayloadBytes)
		if hardPayloadBytes <= fileplugin.HardCapBytes {
			t.Fatalf("setup: hardPayloadBytes %d <= HardCapBytes %d", hardPayloadBytes, fileplugin.HardCapBytes)
		}

		req2 := &abci.RequestFinalizeBlock{
			Height: h2,
			Time:   blockTime,
			Hash:   make([]byte, 32),
			Txs:    [][]byte{rawTx2},
		}
		resp2 := &abci.ResponseFinalizeBlock{
			TxResults: []*abci.ExecTxResult{{Code: 0}},
		}
		dir := t.TempDir()
		sidecarWriteFixture(t, dir, h2, sidecarBuildMeta(t, req2, resp2), nil)

		fpmReg := prometheus.NewRegistry()
		fpm := metrics.NewFilePlugin(fpmReg)

		// Bootstrap must return an error: the hard-cap violation aborts the height.
		_, _, err := fileplugin.Bootstrap(ctx, nats.Client, dir, 0, "pocket", fpm)
		if err == nil {
			t.Fatal("Bootstrap: expected error for hard-cap tx payload, got nil")
		}
		t.Logf("Bootstrap correctly errored: %v", err)

		// Hard cap metric must be 1.
		if got := testutil.ToFloat64(fpm.OversizeRefused); got != 1 {
			t.Errorf("oversize_refused = %.0f, want 1", got)
		}
		// Soft cap must be 0 (hard cap fires first, before the soft cap branch).
		if got := testutil.ToFloat64(fpm.OversizeSoft); got != 0 {
			t.Errorf("oversize_soft = %.0f, want 0 (hard cap fires before soft cap check)", got)
		}

		// Start block consumer; give it time to drain any spurious messages.
		blockRH := startBlockRuntime(t, stream, "block")

		// The envelope for H2 was NEVER published (Bootstrap errored before it).
		// Poll for 3 s (100 ms step) asserting the cursor NEVER advances to H2.
		pollTick := time.NewTicker(100 * time.Millisecond)
		defer pollTick.Stop()
		pollDeadline := time.After(3 * time.Second)
	pollLoop:
		for {
			if cursorAtHeight(t, blockRH.store, "block", h2) {
				t.Fatalf("block cursor advanced to %d; expected it to remain below (hard-cap envelope never published)", h2)
			}
			select {
			case <-pollDeadline:
				break pollLoop
			case <-pollTick.C:
			}
		}

		// No block row for H2.
		var count int
		if err := pg.Pool.QueryRow(ctx, `SELECT count(*) FROM block WHERE height=$1`, h2).Scan(&count); err != nil {
			t.Fatalf("count block h=%d: %v", h2, err)
		}
		if count != 0 {
			t.Errorf("block row count for h=%d = %d, want 0 (hard-cap envelope never published)", h2, count)
		}
	})
}
