package store

import (
	"context"
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

// upgradeHeightsByVersion loads the upgrades table keyed by normalized
// upgrade name (upgrades.name is the chain's dotted tag, e.g. "v0.1.20").
func (s *Store) upgradeHeightsByVersion(ctx context.Context) (map[string]int64, error) {
	ups, err := s.ListUpgrades(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]int64, len(ups))
	for _, u := range ups {
		n, err := protover.Normalize(u.Name)
		if err != nil {
			return nil, fmt.Errorf("upgrades row %q: %w", u.Name, err)
		}
		m[n] = u.AppliedAtHeight
	}
	return m, nil
}

// ConsumerFirstValidHeight resolves spec §4.10 consumer_first_valid_height
// for one version against this network (genesis + upgrades table). Returns
// DormantHeight when the version was never applied and is above genesis.
func (s *Store) ConsumerFirstValidHeight(ctx context.Context, firstValidVersion, genesisVersion string) (int64, error) {
	ups, err := s.upgradeHeightsByVersion(ctx)
	if err != nil {
		return 0, err
	}
	return firstValidHeight(firstValidVersion, genesisVersion, ups)
}

// FirstValidHeights resolves consumer_first_valid_height for every ACTIVE
// consumer. Computed at query time — no materialization (spec §4.10).
func (s *Store) FirstValidHeights(ctx context.Context, genesisVersion string) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT consumer_name, first_valid_version FROM consumer_registry WHERE active = true`)
	if err != nil {
		return nil, fmt.Errorf("query active consumers: %w", err)
	}
	defer rows.Close()
	type rc struct{ name, version string }
	var cons []rc
	for rows.Next() {
		var c rc
		if err := rows.Scan(&c.name, &c.version); err != nil {
			return nil, fmt.Errorf("scan consumer: %w", err)
		}
		cons = append(cons, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ups, err := s.upgradeHeightsByVersion(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(cons))
	for _, c := range cons {
		h, err := firstValidHeight(c.version, genesisVersion, ups)
		if err != nil {
			return nil, fmt.Errorf("consumer %q: %w", c.name, err)
		}
		out[c.name] = h
	}
	return out, nil
}
