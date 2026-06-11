//go:build integration

package integration

// TestBoundaryPipeline exercises the v0.1.26→v0.1.27 upgrade boundary through
// the full sidecar→NATS→consumers pipeline with a DB-driven router.
//
// Heights chosen:
//   - 227782: v0.1.26 era (dir v0_1_20 per README era table); last curated
//     high-activity block in the era (msg_stake×73, events_staked×73, KV×73).
//   - 247894: v0.1.27 era (dir v0_1_27); boundary-proxy quiet block (true
//     boundary 247893 is 15 MB, excluded by >5 MB rule per spec §8.1).
//
// The upgrades table is populated via fixturereport.MainnetUpgrades() upserted
// directly into the store — this is the DB-driven (production) path, not the
// static router used by other supplier tests.  The router is NewDBRouter.
//
// Gap seal decision: 227782 and 247894 are ~20k blocks apart.  The consolidation
// cursor (consumer_consolidation.consolidated_up_to) advances ONLY over a
// contiguous sequence, so it will NOT reach either height when only two isolated
// blocks are bootstrapped.  IsSealed(h, genesis) checks consolidated_up_to >= h
// for ALL registered consumers — which requires the full contiguous sequence from
// genesis up to h.  We therefore assert processed_heights presence for both heights
// (via waitHasProcessed) instead of assertSealed.  This is the correct observability
// for gap-aware non-contiguous fixtures: processed_heights is written atomically with
// the DB commit (invariant 5) and confirms decode+store success without requiring a
// contiguous cursor.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
	supplierhandler "github.com/pokt-network/pocketscribe/internal/consumer/supplier"
	"github.com/pokt-network/pocketscribe/internal/fixturereport"
	pslog "github.com/pokt-network/pocketscribe/internal/log"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// sentinelUpgradeTime is a non-zero placeholder used when the real applied_at_time
// is unavailable in tests.  The column is NOT NULL, so zero-value is rejected.
var sentinelUpgradeTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// startSupplierRuntimeWithDBRouter starts a supplier BatchRuntime backed by a
// DB-driven router loaded from the upgrades table.  This is the production code
// path (NewDBRouter), not NewStaticRouter.  The durable is named
// "supplier-boundary" to avoid colliding with other tests' "supplier" durable.
func startSupplierRuntimeWithDBRouter(t *testing.T, stream jetstream.Stream, ids map[string]int16) *runtimeHandle {
	t.Helper()
	ctx := context.Background()

	s, err := store.New(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	rtr, err := router.NewDBRouter(ctx, s, router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatalf("NewDBRouter: %v", err)
	}

	h := supplierhandler.New(rtr, ids)

	filters := make([]string, 0, 3+len(supplierhandler.EventTypes))
	filters = append(filters, natsx.TxSubjectFilter, natsx.KVSubjectFilter("supplier"), natsx.BlockSubjectFilter)
	for _, et := range supplierhandler.EventTypes {
		filters = append(filters, natsx.EventSubjectFilter(et))
	}
	jsCons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "supplier-boundary",
		FilterSubjects: filters,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     -1,
		AckWait:        60 * time.Second,
		MaxAckPending:  -1,
	})
	if err != nil {
		t.Fatalf("create supplier-boundary consumer: %v", err)
	}
	t.Cleanup(func() { _ = stream.DeleteConsumer(ctx, "supplier-boundary") })

	m := metrics.NewConsumer(prometheus.NewRegistry())
	rt := runtime.NewBatchRuntime(runtime.BatchConfig{
		Handler:  h,
		Store:    s,
		Consumer: jsCons,
		Logger:   pslog.New(io.Discard, slog.LevelError),
		Metrics:  m,
	})
	cancelCtx, cancel := context.WithCancel(ctx)
	rh := &runtimeHandle{
		name:    "supplier-boundary",
		store:   s,
		metrics: m,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go func() {
		defer close(rh.done)
		_ = rt.Run(cancelCtx)
	}()
	t.Cleanup(rh.stop)
	return rh
}

// upsertMainnetUpgrades populates the upgrades table from fixturereport.MainnetUpgrades().
// The store.UpsertUpgrade call is idempotent (ON CONFLICT DO UPDATE).
func upsertMainnetUpgrades(t *testing.T, s *store.Store) {
	t.Helper()
	ctx := context.Background()
	for _, u := range fixturereport.MainnetUpgrades() {
		if err := s.UpsertUpgrade(ctx, store.Upgrade{
			Name:            u.Name,
			AppliedAtHeight: u.AppliedAtHeight,
			AppliedAtTime:   sentinelUpgradeTime,
			DecoderVersion:  u.DecoderVersion,
		}); err != nil {
			t.Fatalf("upsertMainnetUpgrades %s: %v", u.Name, err)
		}
	}
}

// fixturesMustExist fails fast if any of the given file paths are missing.
// Called before the heavier bootstrap step so fixture misconfiguration is
// surfaced immediately with a clear path in the error message.
func fixturesMustExist(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("fixture file missing: %s: %v", p, err)
		}
	}
}

// TestBoundaryPipeline verifies that the v0.1.26→v0.1.27 boundary is decoded
// correctly end-to-end: fileplugin→NATS→block consumer+supplier consumer→DB.
//
// Per the README era table:
//   - height 227782 (dir v0_1_20) → decoder v0_1_20 (v0.1.26 era falls back
//     to v0_1_20, which is the nearest registered decoder ≤ 247892).
//   - height 247894 (dir v0_1_27) → decoder v0_1_27.
//
// Supplier data per expected.json:
//   - 227782: msg_stake×73, events_staked×73, history_operators×73 — active.
//   - 247894: quiet block; expected.json has no supplier fields.
func TestBoundaryPipeline(t *testing.T) {
	pg.Reset(t)
	stream := freshStream(t)

	// Verify fixture files exist on disk before running (fails fast with clear error).
	fixturesMustExist(t,
		"../../test/fixtures/v0_1_20/block-227782-meta",
		"../../test/fixtures/v0_1_20/block-227782-data",
		"../../test/fixtures/v0_1_27/block-247894-meta",
		"../../test/fixtures/v0_1_27/block-247894-data",
	)

	// Populate upgrades table from the chain-authoritative MainnetUpgrades list.
	// This seeds the DB so NewDBRouter can load the real boundary map.
	adminStore, err := store.New(context.Background(), pg.DSN)
	if err != nil {
		t.Fatalf("store.New (admin): %v", err)
	}
	t.Cleanup(adminStore.Close)
	upsertMainnetUpgrades(t, adminStore)

	// Load decoder_version id map (needed by the supplier handler).
	ids := loadDecoderVersionIDs(t)

	// Start the block consumer runtime.
	blockRH := startBlockRuntime(t, stream, "block")

	// Start the supplier runtime with the DB-driven router.
	supplierRH := startSupplierRuntimeWithDBRouter(t, stream, ids)

	// Publish both boundary heights through the real fileplugin bootstrap path.
	bootstrapHeights(t, 227782, 247894)

	// Wait for both consumers to process each height.
	// We use waitHasProcessed (not waitCursor) because 227782 and 247894 are
	// non-contiguous (≈20k block gap).  The contiguous consolidation cursor
	// will not advance past the first height, so waitCursor would deadlock.
	//
	// NOTE: the DB consumer name is handler.ID() = "supplier", NOT the NATS
	// durable name "supplier-boundary".  The durable name is used only for NATS
	// consumer addressing; the store uses handler.ID() for all DB writes.
	for _, h := range []int64{227782, 247894} {
		waitHasProcessed(t, blockRH.store, "block", h, 60*time.Second)
		waitHasProcessed(t, supplierRH.store, "supplier", h, 90*time.Second)
	}

	ctx := context.Background()

	// ── Assert 1: block rows exist for both heights ────────────────────────────
	for _, h := range []int64{227782, 247894} {
		var count int
		if err := pg.Pool.QueryRow(ctx,
			`SELECT count(*) FROM block WHERE height=$1`, h,
		).Scan(&count); err != nil {
			t.Fatalf("count block h=%d: %v", h, err)
		}
		if count != 1 {
			t.Errorf("block row h=%d: count = %d, want 1", h, count)
		}
	}

	// ── Assert 2: supplier rows at 227782 use decoder v0_1_20 ─────────────────
	// Router resolution (per README era table + mainnet_boundaries_test.go):
	//   height 227782 → v0.1.26 era → nearest registered decoder = v0_1_20.
	// The DB-driven router loaded from the upgrades table must resolve identically
	// to the static router exercised in TestDecoderForAllMainnetBoundaries.
	wantDecoder226 := "v0.1.20"
	wantDecoderID226, ok := ids[wantDecoder226]
	if !ok {
		t.Fatalf("decoder_version %s not in DB", wantDecoder226)
	}

	want227782 := loadSupplierExpected(t, "../../test/fixtures/v0_1_20/block-227782-expected.json")

	got227782MsgStake := queryMsgStake(t, supplierRH.store, 227782)
	if len(got227782MsgStake) != len(want227782.MsgStake) {
		t.Errorf("h=227782: msg_stake_supplier count = %d, want %d",
			len(got227782MsgStake), len(want227782.MsgStake))
	}
	for i, row := range got227782MsgStake {
		if row.DecodedBy != wantDecoderID226 {
			t.Errorf("h=227782 msg[%d]: decoded_by_version = %d, want %d (%s)",
				i, row.DecodedBy, wantDecoderID226, wantDecoder226)
		}
	}

	got227782Events := queryEventStaked(t, supplierRH.store, 227782)
	if len(got227782Events) != len(want227782.EventsStaked) {
		t.Errorf("h=227782: event_supplier_staked count = %d, want %d",
			len(got227782Events), len(want227782.EventsStaked))
	}
	// Pre-v0.1.27: supplier embed must be non-null, operator_address must be null.
	for i, row := range got227782Events {
		if len(row.SupplierJSON) == 0 {
			t.Errorf("h=227782 event[%d]: supplier embed IS NULL (want non-null; pre-v0.1.27 shape)", i)
		}
		if row.OperatorAddress != nil && *row.OperatorAddress != "" {
			t.Errorf("h=227782 event[%d]: operator_address IS NOT NULL (want null; pre-v0.1.27 shape), got %q",
				i, *row.OperatorAddress)
		}
	}

	// ── Assert 3: supplier rows at 247894 use decoder v0_1_27 ─────────────────
	// 247894 is the first height in the v0.1.27 era (boundary-proxy; the true
	// boundary 247893 is excluded by the >5 MB rule).  The DB-driven router must
	// resolve it to v0_1_27.
	wantDecoder27 := "v0.1.27"
	wantDecoderID27, ok := ids[wantDecoder27]
	if !ok {
		t.Fatalf("decoder_version %s not in DB", wantDecoder27)
	}

	want247894 := loadSupplierExpected(t, "../../test/fixtures/v0_1_27/block-247894-expected.json")

	// 247894 is a quiet block (no supplier activity per expected.json).
	// Assert zero msg_stake rows — confirming the decoder handled the quiet
	// block without error (BatchRuntime advanced processed_heights).
	got247894MsgStake := queryMsgStake(t, supplierRH.store, 247894)
	if len(got247894MsgStake) != len(want247894.MsgStake) {
		t.Errorf("h=247894: msg_stake_supplier count = %d, want %d (quiet block expected)",
			len(got247894MsgStake), len(want247894.MsgStake))
	}
	// If any rows are unexpectedly present, verify decoder version is correct.
	for i, row := range got247894MsgStake {
		if row.DecodedBy != wantDecoderID27 {
			t.Errorf("h=247894 msg[%d]: decoded_by_version = %d, want %d (%s)",
				i, row.DecodedBy, wantDecoderID27, wantDecoder27)
		}
	}

	got247894Events := queryEventStaked(t, supplierRH.store, 247894)
	if len(got247894Events) != len(want247894.EventsStaked) {
		t.Errorf("h=247894: event_supplier_staked count = %d, want %d (quiet block expected)",
			len(got247894Events), len(want247894.EventsStaked))
	}

	// ── Assert 4: processed_heights rows confirm decode+store committed ────────
	// Gap seal decision (see package comment): assertSealed requires
	// consolidated_up_to >= h for all consumers, which demands a contiguous
	// sequence from genesis to h.  Since we publish only two non-contiguous
	// heights (gap ≈20k blocks), the cursor will not advance.  We assert
	// processed_heights directly — written atomically with the DB commit
	// (invariant 5) — which is the correct observability for gap-aware tests.
	// The DB consumer name is handler.ID() = "supplier" (not the NATS durable).
	for _, h := range []int64{227782, 247894} {
		var blockProcessed bool
		if err := pg.Pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM processed_heights WHERE consumer_name='block' AND height=$1)`, h,
		).Scan(&blockProcessed); err != nil {
			t.Fatalf("processed_heights block h=%d: %v", h, err)
		}
		if !blockProcessed {
			t.Errorf("block consumer: processed_heights missing for h=%d", h)
		}

		var supplierProcessed bool
		if err := pg.Pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM processed_heights WHERE consumer_name='supplier' AND height=$1)`, h,
		).Scan(&supplierProcessed); err != nil {
			t.Fatalf("processed_heights supplier h=%d: %v", h, err)
		}
		if !supplierProcessed {
			t.Errorf("supplier consumer: processed_heights missing for h=%d", h)
		}
	}
}
