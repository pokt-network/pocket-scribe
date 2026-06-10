package consumer

import (
	"context"
	"log/slog"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// dormant reports whether a consumer is dormant on this network (spec §4.10:
// first_valid_version above genesis and never applied → INFINITY). An empty
// genesisVersion disables the gate — network-agnostic callers (and pre-Phase-F
// tests) keep their behavior.
//
// Dormancy is resolved ONCE at startup: wakeup is restart-based (spec test
// 24). Operationally: after `ps sync-upgrades` lands a version that wakes a
// dormant consumer, that consumer must be (re)started to begin consuming.
func dormant(ctx context.Context, st *store.Store, id, firstValid, genesisVersion string, logger *slog.Logger) (bool, error) {
	if genesisVersion == "" {
		return false, nil
	}
	h, err := st.ConsumerFirstValidHeight(ctx, firstValid, genesisVersion)
	if err != nil {
		return false, err
	}
	if h != store.DormantHeight {
		return false, nil
	}
	logger.Info("consumer dormant on this network; exiting cleanly",
		"consumer", id,
		"first_valid_version", firstValid,
		"genesis_decoder_version", genesisVersion)
	return true, nil
}
