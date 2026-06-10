//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
	supplierhandler "github.com/pokt-network/pocketscribe/internal/consumer/supplier"
	pslog "github.com/pokt-network/pocketscribe/internal/log"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// upgradesForFixtures declares the real mainnet upgrade boundaries used by the
// supplier fixture heights.  Applied heights are chain-authoritative from
// docs/research/poktroll-versions.md.  Only versions with registered decoders
// need to appear here — the lenient router falls back for unregistered
// in-between versions.
//
// NOTE: the block consumer tests do NOT include v0.1.8/v0.1.27 boundaries here
// (block header decode is version-invariant). This deliberate asymmetry must
// not be "fixed".
var upgradesForFixtures = []router.Upgrade{
	{Name: "v0.1.8", AppliedAtHeight: 78671, DecoderVersion: "v0_1_8"},
	{Name: "v0.1.9", AppliedAtHeight: 78678, DecoderVersion: "v0_1_9"}, // unregistered → falls back to v0_1_8
	{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
	{Name: "v0.1.11", AppliedAtHeight: 78689, DecoderVersion: "v0_1_11"},  // unregistered → v0_1_10
	{Name: "v0.1.12", AppliedAtHeight: 78697, DecoderVersion: "v0_1_12"},  // unregistered
	{Name: "v0.1.13", AppliedAtHeight: 80510, DecoderVersion: "v0_1_13"},  // unregistered
	{Name: "v0.1.14", AppliedAtHeight: 93825, DecoderVersion: "v0_1_14"},  // unregistered
	{Name: "v0.1.15", AppliedAtHeight: 94370, DecoderVersion: "v0_1_15"},  // non-det window, unregistered
	{Name: "v0.1.16", AppliedAtHeight: 99293, DecoderVersion: "v0_1_16"},  // unregistered
	{Name: "v0.1.17", AppliedAtHeight: 102142, DecoderVersion: "v0_1_17"}, // unregistered → falls back to v0_1_10
	{Name: "v0.1.18", AppliedAtHeight: 116100, DecoderVersion: "v0_1_18"}, // unregistered
	{Name: "v0.1.19", AppliedAtHeight: 117454, DecoderVersion: "v0_1_19"}, // unregistered
	{Name: "v0.1.20", AppliedAtHeight: 135297, DecoderVersion: "v0_1_20"},
	{Name: "v0.1.21", AppliedAtHeight: 138931, DecoderVersion: "v0_1_21"}, // unregistered
	{Name: "v0.1.22", AppliedAtHeight: 155173, DecoderVersion: "v0_1_22"}, // unregistered
	{Name: "v0.1.23", AppliedAtHeight: 161109, DecoderVersion: "v0_1_23"}, // unregistered
	{Name: "v0.1.24", AppliedAtHeight: 161169, DecoderVersion: "v0_1_24"}, // unregistered
	{Name: "v0.1.25", AppliedAtHeight: 190974, DecoderVersion: "v0_1_25"}, // unregistered
	{Name: "v0.1.26", AppliedAtHeight: 190979, DecoderVersion: "v0_1_26"}, // unregistered
	{Name: "v0.1.27", AppliedAtHeight: 247893, DecoderVersion: "v0_1_27"},
	{Name: "v0.1.28", AppliedAtHeight: 287932, DecoderVersion: "v0_1_28"},
	{Name: "v0.1.29", AppliedAtHeight: 382250, DecoderVersion: "v0_1_29"},
}

// startSupplierRuntime mirrors startBlockRuntime but wires a BatchRuntime +
// supplierhandler.Handler with a StaticRouter covering all fixture heights.
// The "supplier" durable subscribes to tx/events/kv/block subjects.
func startSupplierRuntime(t *testing.T, stream jetstream.Stream, ids map[string]int16) *runtimeHandle {
	t.Helper()
	ctx := context.Background()

	s, err := store.New(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	rtr, err := router.NewStaticRouter(upgradesForFixtures, router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatalf("NewStaticRouter: %v", err)
	}

	h := supplierhandler.New(rtr, ids)

	filters := make([]string, 0, 3+len(supplierhandler.EventTypes))
	filters = append(filters, natsx.TxSubjectFilter, natsx.KVSubjectFilter("supplier"), natsx.BlockSubjectFilter)
	for _, et := range supplierhandler.EventTypes {
		filters = append(filters, natsx.EventSubjectFilter(et))
	}
	jsCons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "supplier",
		FilterSubjects: filters,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     -1,
		AckWait:        60 * time.Second,
	})
	if err != nil {
		t.Fatalf("create supplier consumer: %v", err)
	}
	t.Cleanup(func() { _ = stream.DeleteConsumer(ctx, "supplier") })

	m := metrics.NewConsumer(prometheus.NewRegistry())
	rt := runtime.NewBatchRuntime(runtime.BatchConfig{
		Handler:  h,
		Store:    s,
		Consumer: jsCons,
		Logger:   pslog.New(io.Discard, slog.LevelError),
		Metrics:  m,
	})
	cancelCtx, cancel := context.WithCancel(ctx)
	rh := &runtimeHandle{name: "supplier", store: s, metrics: m, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rh.done)
		_ = rt.Run(cancelCtx)
	}()
	t.Cleanup(rh.stop)
	return rh
}

// loadDecoderVersionIDs fetches the decoder_version id map from the shared pg
// container. Supplier tests need this to resolve expected decoded_by_version ids.
func loadDecoderVersionIDs(t *testing.T) map[string]int16 {
	t.Helper()
	ctx := context.Background()
	rows, err := pg.Pool.Query(ctx, `SELECT tag, id FROM decoder_version`)
	if err != nil {
		t.Fatalf("load decoder_version: %v", err)
	}
	defer rows.Close()
	m := map[string]int16{}
	for rows.Next() {
		var tag string
		var id int16
		if err := rows.Scan(&tag, &id); err != nil {
			t.Fatalf("scan decoder_version: %v", err)
		}
		m[tag] = id
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("decoder_version rows: %v", err)
	}
	return m
}

// waitConsumerRegistered polls consumer_registry for the named consumer until it
// appears or the timeout elapses. Used to synchronize Test 21 (the supplier
// runtime registers asynchronously; stopping it before registration would leave
// no row and make IsSealed trivially true).
func waitConsumerRegistered(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	for {
		var exists bool
		if err := pg.Pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM consumer_registry WHERE consumer_name=$1)`, name,
		).Scan(&exists); err == nil && exists {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("waitConsumerRegistered(%s): not registered within %s", name, timeout)
		case <-tick.C:
		}
	}
}

// supplierExpected holds the supplier section of a block-{H}-expected.json.
type supplierExpected struct {
	MsgStake []struct {
		TxIndex         int    `json:"tx_index"`
		OperatorAddress string `json:"operator_address"`
		StakeAmount     int64  `json:"stake_amount"`
		StakeDenom      string `json:"stake_denom"`
	} `json:"msg_stake"`
	EventsStaked []struct {
		TxIndex          int   `json:"tx_index"`
		SessionEndHeight int64 `json:"session_end_height"`
	} `json:"events_staked"`
	HistoryOperators []string `json:"history_operators"`
	SCURowsMin       int      `json:"scu_rows_min"`
}

// loadSupplierExpected reads the supplier section from a fixture expected.json.
func loadSupplierExpected(t *testing.T, path string) supplierExpected {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read expected %s: %v", path, err)
	}
	var outer struct {
		Supplier supplierExpected `json:"supplier"`
	}
	if err := json.Unmarshal(data, &outer); err != nil {
		t.Fatalf("unmarshal expected %s: %v", path, err)
	}
	return outer.Supplier
}

// queryMsgStake returns all msg_stake_supplier rows for block_height h,
// ordered by tx_index, including decoded_by_version.
type msgStakeRow struct {
	TxIndex         int32
	OperatorAddress string
	StakeAmount     int64
	StakeDenom      string
	DecodedBy       int16
}

func queryMsgStake(t *testing.T, s *store.Store, height int64) []msgStakeRow {
	t.Helper()
	ctx := context.Background()
	rows, err := s.Pool().Query(ctx,
		`SELECT tx_index, operator_address, stake_amount, stake_denom, decoded_by_version
		 FROM msg_stake_supplier WHERE block_height=$1 ORDER BY tx_index`, height)
	if err != nil {
		t.Fatalf("query msg_stake_supplier h=%d: %v", height, err)
	}
	defer rows.Close()
	var out []msgStakeRow
	for rows.Next() {
		var r msgStakeRow
		if err := rows.Scan(&r.TxIndex, &r.OperatorAddress, &r.StakeAmount, &r.StakeDenom, &r.DecodedBy); err != nil {
			t.Fatalf("scan msg_stake_supplier: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err msg_stake_supplier h=%d: %v", height, err)
	}
	return out
}

type eventStakedRow struct {
	TxIndex          int32
	SessionEndHeight int64
	OperatorAddress  *string // NULL for pre-v0.1.27 rows
	SupplierJSON     []byte  // NULL for post-v0.1.27 rows
}

func queryEventStaked(t *testing.T, s *store.Store, height int64) []eventStakedRow {
	t.Helper()
	ctx := context.Background()
	rows, err := s.Pool().Query(ctx,
		`SELECT tx_index, session_end_height, operator_address, supplier
		 FROM event_supplier_staked WHERE block_height=$1 ORDER BY tx_index`, height)
	if err != nil {
		t.Fatalf("query event_supplier_staked h=%d: %v", height, err)
	}
	defer rows.Close()
	var out []eventStakedRow
	for rows.Next() {
		var r eventStakedRow
		if err := rows.Scan(&r.TxIndex, &r.SessionEndHeight, &r.OperatorAddress, &r.SupplierJSON); err != nil {
			t.Fatalf("scan event_supplier_staked: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err event_supplier_staked h=%d: %v", height, err)
	}
	return out
}

// Test 18: msg_stake_supplier correctness across 4 fixture versions.
// Pins decoded_by_version to the registered decoder (lenient chain: decision 8).
func TestSupplierMsgDecodeAcrossVersions(t *testing.T) { // spec test 18
	pg.Reset(t)
	stream := freshStream(t)

	ids := loadDecoderVersionIDs(t)

	blockRH := startBlockRuntime(t, stream, "block")
	supplierRH := startSupplierRuntime(t, stream, ids)

	// Bootstrap all 4 positive supplier fixtures.
	bootstrapHeights(t, 102542, 135836, 290584, 385145)

	// Wait for both runtimes to catch up. Heights are non-contiguous so we poll
	// HasProcessed instead of the contiguous cursor (mirrors test 16a pattern).
	for _, h := range []int64{102542, 135836, 290584, 385145} {
		waitCursor(t, blockRH.store, "block", h, 60*time.Second)
		waitCursor(t, supplierRH.store, "supplier", h, 60*time.Second)
	}

	// Expected decoded_by_version ids for each fixture height (lenient fallback
	// chain: 102542 in v0.1.17 era → nearest registered earlier = v0_1_10 → id 110;
	// 135836 → v0_1_20 → 120; 290584 → v0_1_28 → 128; 385145 → v0_1_29 → 129).
	type fixtureCase struct {
		height          int64
		expectedPath    string
		expectedVersion string // decoder_version.tag spelling ("v0.1.10")
	}
	cases := []fixtureCase{
		{102542, "../../test/fixtures/v0_1_10/block-102542-expected.json", "v0.1.10"},
		{135836, "../../test/fixtures/v0_1_20/block-135836-expected.json", "v0.1.20"},
		{290584, "../../test/fixtures/v0_1_28/block-290584-expected.json", "v0.1.28"},
		{385145, "../../test/fixtures/v0_1_29/block-385145-expected.json", "v0.1.29"},
	}

	for _, tc := range cases {
		want := loadSupplierExpected(t, tc.expectedPath)
		wantDecoderID, ok := ids[tc.expectedVersion]
		if !ok {
			t.Fatalf("decoder_version %s not in DB", tc.expectedVersion)
		}

		got := queryMsgStake(t, supplierRH.store, tc.height)
		if len(got) != len(want.MsgStake) {
			t.Errorf("h=%d: msg_stake_supplier count = %d, want %d", tc.height, len(got), len(want.MsgStake))
			continue
		}
		for i, row := range got {
			w := want.MsgStake[i]
			if int(row.TxIndex) != w.TxIndex {
				t.Errorf("h=%d msg[%d]: tx_index = %d, want %d", tc.height, i, row.TxIndex, w.TxIndex)
			}
			if row.OperatorAddress != w.OperatorAddress {
				t.Errorf("h=%d msg[%d]: operator_address = %q, want %q", tc.height, i, row.OperatorAddress, w.OperatorAddress)
			}
			if row.StakeAmount != w.StakeAmount {
				t.Errorf("h=%d msg[%d]: stake_amount = %d, want %d", tc.height, i, row.StakeAmount, w.StakeAmount)
			}
			if row.StakeDenom != w.StakeDenom {
				t.Errorf("h=%d msg[%d]: stake_denom = %q, want %q", tc.height, i, row.StakeDenom, w.StakeDenom)
			}
			if row.DecodedBy != wantDecoderID {
				t.Errorf("h=%d msg[%d]: decoded_by_version = %d, want %d (%s)", tc.height, i, row.DecodedBy, wantDecoderID, tc.expectedVersion)
			}
		}
	}
}

// Test 19: event_supplier_staked rows per height, with era shape verification.
// Pre-v0.1.27 heights must have supplier IS NOT NULL and operator_address IS NULL.
// Post-v0.1.27 heights must have operator_address IS NOT NULL and supplier IS NULL.
func TestSupplierEventStakedAcrossVersions(t *testing.T) { // spec test 19
	pg.Reset(t)
	stream := freshStream(t)

	ids := loadDecoderVersionIDs(t)

	blockRH := startBlockRuntime(t, stream, "block")
	supplierRH := startSupplierRuntime(t, stream, ids)

	bootstrapHeights(t, 102542, 135836, 290584, 385145)

	for _, h := range []int64{102542, 135836, 290584, 385145} {
		waitCursor(t, blockRH.store, "block", h, 60*time.Second)
		waitCursor(t, supplierRH.store, "supplier", h, 60*time.Second)
	}

	type fixtureCase struct {
		height       int64
		expectedPath string
		postV0127    bool // v0.1.27+ shape: operator_address set, supplier NULL
	}
	cases := []fixtureCase{
		{102542, "../../test/fixtures/v0_1_10/block-102542-expected.json", false},
		{135836, "../../test/fixtures/v0_1_20/block-135836-expected.json", false},
		{290584, "../../test/fixtures/v0_1_28/block-290584-expected.json", true},
		{385145, "../../test/fixtures/v0_1_29/block-385145-expected.json", true},
	}

	for _, tc := range cases {
		want := loadSupplierExpected(t, tc.expectedPath)
		got := queryEventStaked(t, supplierRH.store, tc.height)

		if len(got) != len(want.EventsStaked) {
			t.Errorf("h=%d: event_supplier_staked count = %d, want %d", tc.height, len(got), len(want.EventsStaked))
			continue
		}
		for i, row := range got {
			w := want.EventsStaked[i]
			if int(row.TxIndex) != w.TxIndex {
				t.Errorf("h=%d event[%d]: tx_index = %d, want %d", tc.height, i, row.TxIndex, w.TxIndex)
			}
			if row.SessionEndHeight != w.SessionEndHeight {
				t.Errorf("h=%d event[%d]: session_end_height = %d, want %d", tc.height, i, row.SessionEndHeight, w.SessionEndHeight)
			}
			if tc.postV0127 {
				// Post-v0.1.27: operator_address must be set, supplier embed must be NULL.
				if row.OperatorAddress == nil || *row.OperatorAddress == "" {
					t.Errorf("h=%d event[%d]: expected operator_address IS NOT NULL (post-v0.1.27 shape)", tc.height, i)
				}
				if len(row.SupplierJSON) != 0 {
					t.Errorf("h=%d event[%d]: expected supplier IS NULL (post-v0.1.27 shape), got %q", tc.height, i, row.SupplierJSON)
				}
			} else {
				// Pre-v0.1.27: supplier embed must be set, operator_address must be NULL.
				if len(row.SupplierJSON) == 0 {
					t.Errorf("h=%d event[%d]: expected supplier IS NOT NULL (pre-v0.1.27 shape)", tc.height, i)
				}
				if row.OperatorAddress != nil && *row.OperatorAddress != "" {
					t.Errorf("h=%d event[%d]: expected operator_address IS NULL (pre-v0.1.27 shape), got %q", tc.height, i, *row.OperatorAddress)
				}
			}
		}
	}
}

// Test 20: KV history (supplier_history + SCU) append-only and out-of-order.
// (a) one supplier_history row per operator with services IS NULL (dehydrated era).
// (b) scu_rows_min SCU rows present.
// (c) commutativity: bootstrap 135837 BEFORE 135836 and assert identical rows.
func TestSupplierKVHistoryAppendOnly(t *testing.T) { // spec test 20
	// ── subtest (a+b): normal ordering ──────────────────────────────────────────
	t.Run("normal_order", func(t *testing.T) {
		pg.Reset(t)
		stream := freshStream(t)

		ids := loadDecoderVersionIDs(t)

		blockRH := startBlockRuntime(t, stream, "block")
		supplierRH := startSupplierRuntime(t, stream, ids)

		bootstrapHeights(t, 102542, 135836, 290584, 385145)

		for _, h := range []int64{102542, 135836, 290584, 385145} {
			waitCursor(t, blockRH.store, "block", h, 60*time.Second)
			waitCursor(t, supplierRH.store, "supplier", h, 60*time.Second)
		}

		type fixtureCase struct {
			height       int64
			expectedPath string
		}
		cases := []fixtureCase{
			{102542, "../../test/fixtures/v0_1_10/block-102542-expected.json"},
			{135836, "../../test/fixtures/v0_1_20/block-135836-expected.json"},
			{290584, "../../test/fixtures/v0_1_28/block-290584-expected.json"},
			{385145, "../../test/fixtures/v0_1_29/block-385145-expected.json"},
		}

		ctx := context.Background()
		for _, tc := range cases {
			want := loadSupplierExpected(t, tc.expectedPath)

			// (a) supplier_history: one row per expected operator, services IS NULL.
			rows, err := supplierRH.store.Pool().Query(ctx,
				`SELECT operator_address, services FROM supplier_history
				 WHERE block_height=$1 ORDER BY operator_address`, tc.height)
			if err != nil {
				t.Fatalf("h=%d query supplier_history: %v", tc.height, err)
			}
			var gotOperators []string
			var nonNullServices int
			for rows.Next() {
				var op string
				var svc []byte
				if err := rows.Scan(&op, &svc); err != nil {
					t.Fatalf("h=%d scan supplier_history: %v", tc.height, err)
				}
				gotOperators = append(gotOperators, op)
				if len(svc) > 0 {
					nonNullServices++
				}
			}
			rows.Close()
			if rows.Err() != nil {
				t.Fatalf("h=%d supplier_history rows: %v", tc.height, rows.Err())
			}

			wantOps := append([]string(nil), want.HistoryOperators...)
			sort.Strings(wantOps)
			sort.Strings(gotOperators)
			if len(gotOperators) != len(wantOps) {
				t.Errorf("h=%d supplier_history count = %d, want %d", tc.height, len(gotOperators), len(wantOps))
			} else {
				for i := range wantOps {
					if gotOperators[i] != wantOps[i] {
						t.Errorf("h=%d supplier_history[%d]: operator = %q, want %q", tc.height, i, gotOperators[i], wantOps[i])
					}
				}
			}
			// All fixture heights are in the dehydrated era (v0.1.8+): services MUST be NULL.
			if nonNullServices > 0 {
				t.Errorf("h=%d supplier_history: %d rows have non-NULL services (should be NULL — dehydrated era)", tc.height, nonNullServices)
			}

			// (b) SCU rows: count must be at least scu_rows_min.
			var scuCount int
			if err := supplierRH.store.Pool().QueryRow(ctx,
				`SELECT count(*) FROM supplier_service_config_update_history WHERE block_height=$1`, tc.height,
			).Scan(&scuCount); err != nil {
				t.Fatalf("h=%d count SCU: %v", tc.height, err)
			}
			if scuCount < want.SCURowsMin {
				t.Errorf("h=%d SCU rows = %d, want >= %d", tc.height, scuCount, want.SCURowsMin)
			}
		}
	})

	// ── subtest (c): commutativity (135837 published before 135836) ─────────────
	// The block 135837 fixture is in the v0_1_20 directory (same tarball / era).
	t.Run("out_of_order_commutativity", func(t *testing.T) {
		// Check that 135837 fixture files exist; skip gracefully if not.
		meta135837 := "../../test/fixtures/v0_1_20/block-135837-meta"
		if _, err := os.Stat(meta135837); os.IsNotExist(err) {
			t.Skip("block-135837-meta fixture not present; skipping commutativity sub-test")
		}

		pg.Reset(t)
		stream := freshStream(t)

		ids := loadDecoderVersionIDs(t)

		blockRH := startBlockRuntime(t, stream, "block")
		supplierRH := startSupplierRuntime(t, stream, ids)

		// Bootstrap 135837 FIRST (out-of-order w.r.t. 135836).
		bootstrapHeights(t, 135837, 135836)

		waitCursor(t, blockRH.store, "block", 135837, 60*time.Second)
		waitCursor(t, supplierRH.store, "supplier", 135837, 60*time.Second)

		// Collect supplier_history for 135836 under the out-of-order arrival.
		ctx := context.Background()
		rows, err := supplierRH.store.Pool().Query(ctx,
			`SELECT operator_address FROM supplier_history WHERE block_height=135836 ORDER BY operator_address`)
		if err != nil {
			t.Fatalf("query supplier_history 135836: %v", err)
		}
		var ooOps []string
		for rows.Next() {
			var op string
			if err := rows.Scan(&op); err != nil {
				t.Fatalf("scan: %v", err)
			}
			ooOps = append(ooOps, op)
		}
		rows.Close()

		// Now compare with the in-order expected operators.
		want := loadSupplierExpected(t, "../../test/fixtures/v0_1_20/block-135836-expected.json")
		wantOps := append([]string(nil), want.HistoryOperators...)
		sort.Strings(wantOps)
		sort.Strings(ooOps)
		if len(ooOps) != len(wantOps) {
			t.Errorf("out-of-order 135836: supplier_history count = %d, want %d", len(ooOps), len(wantOps))
		} else {
			for i := range wantOps {
				if ooOps[i] != wantOps[i] {
					t.Errorf("out-of-order 135836: operator[%d] = %q, want %q", i, ooOps[i], wantOps[i])
				}
			}
		}
	})
}

// Test 21: AND-seal with supplier lag + quiet heights.
// Steps mirror the plan's description:
//  1. Start supplier runtime (so it registers) then immediately stop it.
//  2. Bootstrap v0_1_0 heights {1,2,3} (negative fixtures — zero supplier rows).
//  3. Start block runtime; waitCursor(block, 3); assert NOT sealed (supplier row
//     present with cursor 0 prevents sealing).
//  4. Restart supplier runtime; waitCursor(supplier, 3).
//  5. Assert sealed AND zero data rows (quiet heights advanced the cursor; ADR-024).
func TestSupplierANDSealWithQuietHeights(t *testing.T) { // spec test 21
	pg.Reset(t)
	stream := freshStream(t)

	ids := loadDecoderVersionIDs(t)

	// Step 1: start the supplier runtime so it self-registers in consumer_registry,
	// then immediately stop it before any bootstrap messages arrive.
	supplierRH1 := startSupplierRuntime(t, stream, ids)
	waitConsumerRegistered(t, "supplier", 5*time.Second)
	supplierRH1.stop()

	// Step 2: bootstrap the 3 v0.1.0 negative fixtures (heights 1, 2, 3).
	blockRH := startBlockRuntime(t, stream, "block")
	bootstrapHeights(t, 1, 2, 3)

	// Step 3: wait for block cursor to reach 3, then assert NOT sealed because
	// supplier is registered but its cursor is still 0.
	waitCursor(t, blockRH.store, "block", 3, 20*time.Second)
	assertSealed(t, blockRH.store, 3, false)

	// Step 4: restart the supplier runtime against the same durable
	// (DeliverAllPolicy → redelivers from seq 0; dedup absorbs duplicates
	// for the block subjects since it's a fresh stream anyway).
	supplierRH2 := startSupplierRuntime(t, stream, ids)

	// Step 5a: wait for supplier cursor to reach 3.
	waitCursor(t, supplierRH2.store, "supplier", 3, 30*time.Second)

	// Step 5b: sealed now — both consumers past height 3.
	assertSealed(t, supplierRH2.store, 3, true)

	// Step 5c: quiet heights produced zero data rows (decisions 4 + ADR-024).
	ctx := context.Background()
	var supplierHistoryCount, msgStakeCount int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM supplier_history WHERE block_height IN (1,2,3)`,
	).Scan(&supplierHistoryCount); err != nil {
		t.Fatalf("count supplier_history: %v", err)
	}
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM msg_stake_supplier WHERE block_height IN (1,2,3)`,
	).Scan(&msgStakeCount); err != nil {
		t.Fatalf("count msg_stake_supplier: %v", err)
	}
	if supplierHistoryCount != 0 {
		t.Errorf("supplier_history rows at quiet heights = %d, want 0", supplierHistoryCount)
	}
	if msgStakeCount != 0 {
		t.Errorf("msg_stake_supplier rows at quiet heights = %d, want 0", msgStakeCount)
	}
}
