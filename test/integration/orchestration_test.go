//go:build integration

package integration

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
	"github.com/pokt-network/pocketscribe/internal/config"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// genesisV0_1_0 is the mainnet genesis decoder version, in decoder-dir
// spelling on purpose — exercises protover normalization at every call site.
const genesisV0_1_0 = "v0_1_0"

func requiredSet(t *testing.T, h int64, genesis string) []string {
	t.Helper()
	s := storeFrom(t)
	names, err := s.RequiredSet(context.Background(), h, genesis)
	if err != nil {
		t.Fatalf("RequiredSet(%d): %v", h, err)
	}
	return names
}

func TestDynamicRequiredSetPerHeight(t *testing.T) { // spec test 23 (§11.1)
	pg.Reset(t)
	s := storeFrom(t)
	mustRegister(t, s, "blocklike", "v0.1.0")
	mustRegister(t, s, "late", "v0.1.20")
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")

	// H < first_valid: the late consumer is NOT in required_set…
	if got := requiredSet(t, 135296, genesisV0_1_0); !slices.Equal(got, []string{"blocklike"}) {
		t.Fatalf("required_set(135296) = %v, want [blocklike]", got)
	}
	// …and H ≥ first_valid: it is.
	if got := requiredSet(t, 135297, genesisV0_1_0); !slices.Equal(got, []string{"blocklike", "late"}) {
		t.Fatalf("required_set(135297) = %v, want [blocklike late]", got)
	}

	// Sealing follows: H seals WITHOUT the late consumer below its first_valid…
	setConsolidation(t, "blocklike", 200000)
	assertSealed(t, s, 135296, genesisV0_1_0, true)
	// …but not at/after it until the late consumer catches up.
	assertSealed(t, s, 135297, genesisV0_1_0, false)
	setConsolidation(t, "late", 135297)
	assertSealed(t, s, 135297, genesisV0_1_0, true)
}

func orchJSConsumer(t *testing.T, durable string) jetstream.Consumer {
	t.Helper()
	ctx := context.Background()
	stream, err := nats.Client.EnsureStream(ctx, 2*time.Minute)
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	c, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: natsx.BlockSubjectFilter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    -1,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	return c
}

func newOrchRuntime(t *testing.T, s *store.Store, id, firstValid, genesis string) *runtime.Runtime {
	t.Helper()
	return runtime.NewRuntime(runtime.Config{
		Handler:        runtime.NewNoOpHandler(id, firstValid),
		Store:          s,
		Consumer:       orchJSConsumer(t, id),
		Logger:         slog.Default(),
		Metrics:        metrics.NewConsumer(prometheus.NewRegistry()),
		GenesisVersion: genesis,
	})
}

func TestDormantConsumer(t *testing.T) { // spec test 22 (§11.1)
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	mustRegister(t, s, "blocklike", "v0.1.0")

	// Fictitious consumer: v0.2.0 was never applied on this network.
	rt := newOrchRuntime(t, s, "phantom", "v0.2.0", genesisV0_1_0)
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := rt.Run(runCtx); err != nil {
		t.Fatalf("dormant consumer must exit cleanly, got %v", err)
	}
	if runCtx.Err() != nil {
		t.Fatal("Run consumed until timeout — dormancy gate did not fire")
	}

	// It registered…
	var active bool
	if err := pg.Pool.QueryRow(ctx,
		`SELECT active FROM consumer_registry WHERE consumer_name='phantom'`).Scan(&active); err != nil {
		t.Fatalf("phantom not registered: %v", err)
	}
	if !active {
		t.Fatal("phantom must register active (dormancy is height-derived, not a flag)")
	}
	// …but affects no height's required_set.
	for _, h := range []int64{1, 1_000_000} {
		if got := requiredSet(t, h, genesisV0_1_0); slices.Contains(got, "phantom") {
			t.Fatalf("required_set(%d) contains dormant phantom: %v", h, got)
		}
	}
}

func TestConsumerWakeup(t *testing.T) { // spec test 24 (§11.1)
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// CAUTION: Runtime.Run returns nil BOTH on dormancy and on clean ctx
	// cancellation (internal/consumer/runtime.go:78) — dormant vs awake is
	// distinguished by ELAPSED TIME, not by the returned error.

	// Run 1: dormant — must return well before the deadline.
	rt := newOrchRuntime(t, s, "phantom", "v0.2.0", genesisV0_1_0)
	run1Ctx, cancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel1()
	if err := rt.Run(run1Ctx); err != nil {
		t.Fatalf("run 1 (dormant): %v", err)
	}
	if run1Ctx.Err() != nil {
		t.Fatal("run 1 consumed until the deadline — dormancy gate did not fire")
	}

	// sync-upgrades lands the new version (different router/upgrades state
	// between runs, per the spec test note).
	seedUpgrade(t, s, "v0.2.0", 500000, "v0_2_0")

	// required_set flips exactly at the applied height.
	if got := requiredSet(t, 499999, genesisV0_1_0); slices.Contains(got, "phantom") {
		t.Fatalf("phantom required before first_valid: %v", got)
	}
	if got := requiredSet(t, 500000, genesisV0_1_0); !slices.Contains(got, "phantom") {
		t.Fatalf("phantom missing from required_set(500000): %v", got)
	}

	// Run 2: awake — the gate passes and the runtime consumes (idle) until
	// the deadline. Awake ⇒ Run occupies (nearly) the whole window.
	rt2 := newOrchRuntime(t, s, "phantom", "v0.2.0", genesisV0_1_0)
	const window = 3 * time.Second
	run2Ctx, cancel2 := context.WithTimeout(ctx, window)
	defer cancel2()
	start := time.Now()
	if err := rt2.Run(run2Ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run 2 (awake): unexpected error %v", err)
	}
	// Generous margin: an awake Run cannot return until the ctx fires (it only
	// returns early on dormancy or a non-ctx error, both caught above), so any
	// elapsed >= window-1s proves the gate passed; a dormant exit takes ~ms.
	if elapsed := time.Since(start); elapsed < window-time.Second {
		t.Fatalf("run 2 returned after %v — consumer did not wake (exited as dormant)", elapsed)
	}
}

func TestMultiNetworkRequiredSet(t *testing.T) { // spec test 25 (§11.1)
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// The REAL network configs — if their genesis versions drift this test
	// must fail loud, not silently keep passing.
	mainnet, err := config.Load("../../configs/networks/mainnet.yaml")
	if err != nil {
		t.Fatalf("load mainnet.yaml: %v", err)
	}
	localnet, err := config.Load("../../configs/networks/localnet.yaml")
	if err != nil {
		t.Fatalf("load localnet.yaml: %v", err)
	}
	if mainnet.Network.GenesisDecoderVersion == localnet.Network.GenesisDecoderVersion {
		t.Fatal("test premise broken: mainnet and localnet genesis versions are equal")
	}

	// Same consumer code, same registration…
	mustRegister(t, s, "midver", "v0.1.20")
	// …mainnet state: v0.1.20 applied at 135297 (what ps sync-upgrades writes).
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")

	// Mainnet (genesis v0_1_0): valid only from the upgrade height.
	h, err := s.ConsumerFirstValidHeight(ctx, "v0.1.20", mainnet.Network.GenesisDecoderVersion)
	if err != nil || h != 135297 {
		t.Fatalf("mainnet first_valid = %d, %v; want 135297", h, err)
	}
	if got := requiredSet(t, 1, mainnet.Network.GenesisDecoderVersion); slices.Contains(got, "midver") {
		t.Fatalf("mainnet required_set(1) must exclude midver: %v", got)
	}

	// Localnet (genesis v0_1_33 ≥ v0.1.20): valid from height 1, no upgrade row needed.
	h, err = s.ConsumerFirstValidHeight(ctx, "v0.1.20", localnet.Network.GenesisDecoderVersion)
	if err != nil || h != 1 {
		t.Fatalf("localnet first_valid = %d, %v; want 1", h, err)
	}
	if got := requiredSet(t, 1, localnet.Network.GenesisDecoderVersion); !slices.Contains(got, "midver") {
		t.Fatalf("localnet required_set(1) must include midver: %v", got)
	}
}

func TestBackfillSemantics(t *testing.T) { // spec test 26 (§11.1)
	pg.Reset(t)
	s := storeFrom(t)

	// Established network state: one consumer, consolidated far ahead.
	mustRegister(t, s, "blocklike", "v0.1.0")
	setConsolidation(t, "blocklike", 150000)
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")
	assertSealed(t, s, 150000, genesisV0_1_0, true)

	// A consumer added "after the fact": its duty starts at first_valid_height
	// (135297) — it has no consolidation row yet (cursor effectively starts there).
	mustRegister(t, s, "late", "v0.1.20")

	// Seals before its first_valid are unaffected…
	assertSealed(t, s, 135296, genesisV0_1_0, true)
	// …seals at/after pause until the backfill catches up…
	assertSealed(t, s, 135297, genesisV0_1_0, false)
	assertSealed(t, s, 150000, genesisV0_1_0, false)
	// …and resume exactly as far as the late consumer has consolidated.
	setConsolidation(t, "late", 140000)
	assertSealed(t, s, 140000, genesisV0_1_0, true)
	assertSealed(t, s, 150000, genesisV0_1_0, false)
	setConsolidation(t, "late", 150000)
	assertSealed(t, s, 150000, genesisV0_1_0, true)
}
