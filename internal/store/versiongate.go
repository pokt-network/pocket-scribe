package store

import (
	"fmt"
	"math"

	"github.com/pokt-network/pocketscribe/internal/protover"
)

// DormantHeight is the spec §4.10 INFINITY sentinel: a consumer whose
// first_valid_version is above the network genesis and not present in the
// upgrades table is dormant on this network — required at no height.
const DormantHeight int64 = math.MaxInt64

// firstValidHeight implements consumer_first_valid_height(c, network) from
// spec §4.10. upgradeHeights is keyed by protover-Normalized upgrade name.
func firstValidHeight(firstValid, genesis string, upgradeHeights map[string]int64) (int64, error) {
	v, err := protover.Normalize(firstValid)
	if err != nil {
		return 0, fmt.Errorf("first_valid_version: %w", err)
	}
	g, err := protover.Normalize(genesis)
	if err != nil {
		return 0, fmt.Errorf("genesis_decoder_version: %w", err)
	}
	if protover.Compare(v, g) <= 0 {
		return 1, nil
	}
	if h, ok := upgradeHeights[v]; ok {
		return h, nil
	}
	return DormantHeight, nil
}
