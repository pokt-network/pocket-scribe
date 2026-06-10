//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
	blockhandler "github.com/pokt-network/pocketscribe/internal/consumer/block"
	"github.com/pokt-network/pocketscribe/internal/fileplugin"
	pslog "github.com/pokt-network/pocketscribe/internal/log"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	"github.com/pokt-network/pocketscribe/internal/store"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// blockStoreInserter adapts store.InsertBlock to blockhandler.Inserter.
type blockStoreInserter struct{}

func (blockStoreInserter) InsertBlock(ctx context.Context, tx pgx.Tx, h *types.BlockHeader) error {
	return store.InsertBlock(ctx, tx, h)
}

// startBlockRuntime mirrors startRuntime but wires the block handler instead of NoOp.
// Each runtime gets its own prometheus.Registry to avoid MustRegister panics when
// multiple runtimes run in the same test process.
func startBlockRuntime(t *testing.T, stream jetstream.Stream, name string) *runtimeHandle {
	t.Helper()
	s, err := store.New(context.Background(), pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	cons := durableConsumer(t, stream, name, 2*time.Second)
	m := metrics.NewConsumer(prometheus.NewRegistry())
	h := blockhandler.New(blockStoreInserter{})
	rt := runtime.NewRuntime(runtime.Config{
		Handler:  h,
		Store:    s,
		Consumer: cons,
		Logger:   pslog.New(io.Discard, slog.LevelError),
		Metrics:  m,
	})
	ctx, cancel := context.WithCancel(context.Background())
	rh := &runtimeHandle{name: name, store: s, metrics: m, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rh.done)
		_ = rt.Run(ctx)
	}()
	t.Cleanup(rh.stop)
	return rh
}

// bootstrapHeights copies the block-{H}-{meta,data} fixture pairs for the
// given heights into a temp dir and runs fileplugin.Bootstrap against it.
// It searches all fixture subdirectories for the files. Any error is fatal.
func bootstrapHeights(t *testing.T, heights ...int64) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()

	fixtureBase := filepath.Join("..", "..", "test", "fixtures")
	for _, h := range heights {
		hStr := int64str(h)
		metaName := "block-" + hStr + "-meta"
		dataName := "block-" + hStr + "-data"

		// Search fixture subdirectories for the height's files.
		var metaSrc, dataSrc string
		entries, err := os.ReadDir(fixtureBase)
		if err != nil {
			t.Fatalf("read fixtures dir: %v", err)
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			candidate := filepath.Join(fixtureBase, e.Name(), metaName)
			if _, err := os.Stat(candidate); err == nil {
				metaSrc = candidate
				dataSrc = filepath.Join(fixtureBase, e.Name(), dataName)
				break
			}
		}
		if metaSrc == "" {
			t.Fatalf("bootstrapHeights: no fixture found for height %d", h)
		}

		copyFile(t, metaSrc, filepath.Join(dir, metaName))
		copyFile(t, dataSrc, filepath.Join(dir, dataName))
	}

	heights2, msgs, err := fileplugin.Bootstrap(ctx, nats.Client, dir, 0, "pocket")
	if err != nil {
		t.Fatalf("fileplugin.Bootstrap: %v", err)
	}
	t.Logf("bootstrapHeights: %d heights, %d messages", heights2, msgs)
}

// copyFile copies src → dst; fatal on any error.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("copyFile read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("copyFile write %s: %v", dst, err)
	}
}

// expectedBlock holds the expected values from a block-{H}-expected.json fixture.
type expectedBlock struct {
	Height          int64     `json:"height"`
	Time            time.Time `json:"time"`
	Hash            string    `json:"hash"`
	ProposerAddress string    `json:"proposer_address"`
	TxCount         int       `json:"tx_count"`
}

// loadExpected reads and parses test/fixtures/<vdir>/block-{H}-expected.json.
func loadExpected(t *testing.T, path string) expectedBlock {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read expected: %v", err)
	}
	var e expectedBlock
	if err := json.Unmarshal(data, &e); err != nil {
		t.Fatalf("unmarshal expected: %v", err)
	}
	return e
}

// waitBlockRow polls block WHERE height=$h until the row appears (timeout: 10s).
func waitBlockRow(t *testing.T, s *store.Store, h int64, timeout time.Duration) expectedBlock {
	t.Helper()
	ctx := context.Background()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	for {
		var got expectedBlock
		err := s.Pool().QueryRow(ctx,
			`SELECT height, time, hash, proposer_address, tx_count FROM block WHERE height=$1`, h,
		).Scan(&got.Height, &got.Time, &got.Hash, &got.ProposerAddress, &got.TxCount)
		if err == nil {
			return got
		}
		select {
		case <-deadline:
			t.Fatalf("waitBlockRow(%d): row not found within %s", h, timeout)
		case <-tick.C:
		}
	}
}

// Test 16a: block-row correctness against 5 boundary real fixtures.
// Heights are non-contiguous so no consolidation/seal assertion is made.
func TestBlockConsumerRowCorrectness(t *testing.T) { // spec test 16a
	pg.Reset(t)
	stream := freshStream(t)

	bh := startBlockRuntime(t, stream, "block")

	// Fixtures: one per version boundary (v0_1_0@1, v0_1_10@78683, v0_1_20@135297, v0_1_28@287932, v0_1_29@382250).
	type fixtureCase struct {
		height       int64
		expectedPath string
	}
	cases := []fixtureCase{
		{1, "../../test/fixtures/v0_1_0/block-1-expected.json"},
		{78683, "../../test/fixtures/v0_1_10/block-78683-expected.json"},
		{135297, "../../test/fixtures/v0_1_20/block-135297-expected.json"},
		{287932, "../../test/fixtures/v0_1_28/block-287932-expected.json"},
		{382250, "../../test/fixtures/v0_1_29/block-382250-expected.json"},
	}

	// Bootstrap all heights via the real fan-out pipeline.
	bootstrapHeights(t, 1, 78683, 135297, 287932, 382250)

	for _, tc := range cases {
		want := loadExpected(t, tc.expectedPath)
		got := waitBlockRow(t, bh.store, tc.height, 10*time.Second)

		if got.Height != want.Height {
			t.Errorf("height %d: Height = %d, want %d", tc.height, got.Height, want.Height)
		}
		// Compare timestamps in UTC truncated to microseconds: Postgres stores
		// TIMESTAMPTZ with microsecond precision; the expected JSON may carry
		// nanoseconds from the decoded header.
		gotUTC := got.Time.UTC().Truncate(time.Microsecond)
		wantUTC := want.Time.UTC().Truncate(time.Microsecond)
		if !gotUTC.Equal(wantUTC) {
			t.Errorf("height %d: Time = %v, want %v", tc.height, gotUTC, wantUTC)
		}
		if got.Hash != want.Hash {
			t.Errorf("height %d: Hash = %q, want %q", tc.height, got.Hash, want.Hash)
		}
		if got.ProposerAddress != want.ProposerAddress {
			t.Errorf("height %d: ProposerAddress = %q, want %q", tc.height, got.ProposerAddress, want.ProposerAddress)
		}
		if got.TxCount != want.TxCount {
			t.Errorf("height %d: TxCount = %d, want %d", tc.height, got.TxCount, want.TxCount)
		}
	}
}

// Test 16b: AND-seal with 3 contiguous v0.1.0 blocks (1, 2, 3).
// Both block consumer + a NoOp consumer must advance their cursors to 3 before
// IsSealed(3) returns true.
func TestBlockConsumerANDSeal(t *testing.T) { // spec test 16b
	pg.Reset(t)
	stream := freshStream(t)

	blockRH := startBlockRuntime(t, stream, "block")
	noopRH := startRuntime(t, stream, "noop-a")

	// Bootstrap 3 contiguous real v0.1.0 block fixtures via the real pipeline.
	bootstrapHeights(t, 1, 2, 3)

	// Wait for both cursors to reach 3.
	waitCursor(t, blockRH.store, "block", 3, 15*time.Second)
	waitCursor(t, noopRH.store, "noop-a", 3, 15*time.Second)

	// Both required consumers passed height 3 → it must be sealed.
	assertSealed(t, blockRH.store, 3, true)
}

// Test 17: self-heal — bootstrap heights 1 and 3 first (gap at 2), assert
// the cursor freezes at 1, then bootstrap height 2 and assert recovery to 3
// with all 3 block rows present. Dedup makes re-publishing heights 1 and 3
// safe.
func TestBlockConsumerSelfHealGap(t *testing.T) { // spec test 17
	pg.Reset(t)
	stream := freshStream(t)

	bh := startBlockRuntime(t, stream, "block")

	// Bootstrap heights 1 and 3 — height 2 is missing (gap).
	bootstrapHeights(t, 1, 3)

	// Wait for cursor to advance to 1 (height 1 processed and consolidated).
	waitCursor(t, bh.store, "block", 1, 10*time.Second)

	// Give the runtime time to process height 3 and freeze.
	time.Sleep(750 * time.Millisecond)

	ctx := context.Background()
	if cur, _ := bh.store.ConsolidatedUpTo(ctx, "block"); cur != 1 {
		t.Fatalf("cursor = %d, want frozen at 1 (gap at height 2)", cur)
	}

	// Fill the gap: bootstrap height 2 (dedup absorbs 1 and 3 if they re-appear).
	bootstrapHeights(t, 2)

	// Cursor must now advance to 3.
	waitCursor(t, bh.store, "block", 3, 15*time.Second)

	// All three block rows must be present.
	for _, h := range []int64{1, 2, 3} {
		var count int
		if err := bh.store.Pool().QueryRow(ctx,
			`SELECT count(*) FROM block WHERE height=$1`, h).Scan(&count); err != nil {
			t.Fatalf("count block height %d: %v", h, err)
		}
		if count != 1 {
			t.Fatalf("block height %d: count = %d, want 1", h, count)
		}
	}
}

// int64str formats an int64 without importing strconv at the top level.
func int64str(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 20)
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
