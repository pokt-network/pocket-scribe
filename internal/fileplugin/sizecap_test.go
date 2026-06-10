package fileplugin

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/pokt-network/pocketscribe/internal/metrics"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestCapPublishPolicy(t *testing.T) { // spec test 27 (§11.1)
	reg := prometheus.NewRegistry()
	fpm := metrics.NewFilePlugin(reg)
	var published int
	rec := func(_ string, _ []byte, _ string) error { published++; return nil }
	p := capPublish(rec, discardLogger(), fpm)

	// Small payload: published, no counters.
	if err := p("pokt.tx.1.0", make([]byte, 1024), "a"); err != nil {
		t.Fatal(err)
	}
	// Exactly AT the soft cap: still silent (cap is exclusive).
	if err := p("pokt.tx.1.1", make([]byte, SoftCapBytes), "b"); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(fpm.OversizeSoft); got != 0 {
		t.Fatalf("soft counter = %v after at-cap payload, want 0", got)
	}
	// Above soft cap: WARN + counter, still published.
	if err := p("pokt.tx.1.2", make([]byte, SoftCapBytes+1), "c"); err != nil {
		t.Fatalf("soft-cap payload must still publish: %v", err)
	}
	if got := testutil.ToFloat64(fpm.OversizeSoft); got != 1 {
		t.Fatalf("soft counter = %v, want 1", got)
	}
	// Above hard cap: refused with error, counter, NOT published.
	before := published
	if err := p("pokt.tx.1.3", make([]byte, HardCapBytes+1), "d"); err == nil {
		t.Fatal("hard-cap payload must be refused")
	}
	if published != before {
		t.Fatal("hard-cap payload must not reach the inner publish")
	}
	if got := testutil.ToFloat64(fpm.OversizeRefused); got != 1 {
		t.Fatalf("refused counter = %v, want 1", got)
	}
}

func TestCapPublishNilMetrics(t *testing.T) {
	// Tests pass nil metrics — the wrapper must not panic.
	p := capPublish(func(_ string, _ []byte, _ string) error { return nil }, discardLogger(), nil)
	if err := p("s", make([]byte, SoftCapBytes+1), "m"); err != nil {
		t.Fatal(err)
	}
	if err := p("s", make([]byte, HardCapBytes+1), "m"); err == nil {
		t.Fatal("want refusal")
	}
}

func TestFanOutHeightRefusesOversizeTx(t *testing.T) { // spec test 27, fan-out path
	dir := t.TempDir()
	// RequestFinalizeBlock with a 1 MiB+1 tx; minimal valid header fields.
	req := &abci.RequestFinalizeBlock{
		Height: 42,
		Time:   time.Date(2025, 6, 17, 0, 0, 0, 0, time.UTC),
		Hash:   make([]byte, 32),
		Txs:    [][]byte{make([]byte, HardCapBytes+1)},
	}
	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	resp := &abci.ResponseFinalizeBlock{TxResults: []*abci.ExecTxResult{{Code: 0}}}
	meta := buildThreeRecordMetaWithPayloads(t, reqBytes, mustMarshalResp(t, resp))
	metaPath := filepath.Join(dir, "block-42-meta")
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatal(err)
	}
	// fanOutHeight also reads the data file; create an empty one.
	dataPath := filepath.Join(dir, "block-42-data")
	if err := os.WriteFile(dataPath, []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	p := capPublish(func(_ string, _ []byte, _ string) error { return nil }, discardLogger(), nil)
	if _, err := fanOutHeight(context.Background(), p, 42, metaPath, "pocket"); err == nil {
		t.Fatal("oversize tx must abort the height")
	}
}
