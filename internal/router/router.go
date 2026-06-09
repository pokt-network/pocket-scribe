package router

import (
	"fmt"
	"sort"

	"github.com/pokt-network/pocketscribe/internal/decoders"
)

// Upgrade is one height→decoder boundary loaded from the upgrades table.
type Upgrade struct {
	Name            string
	AppliedAtHeight int64
	DecoderVersion  string
}

// Router resolves a block height to the decoder version active at that height.
type Router interface {
	DecoderFor(height int64) (decoders.Decoder, error)
}

// staticRouter is an in-memory height→decoder map (no DB). Used by NewStaticRouter
// and as the resolved snapshot inside the DB-driven router.
type staticRouter struct {
	upgrades       []Upgrade // sorted ascending by AppliedAtHeight
	registry       map[string]decoders.Decoder
	genesisVersion string
}

// NewStaticRouter builds a router from a per-network upgrade set (data) + a
// version-keyed decoder registry. It does NOT require every upgrade's version to
// be registered — unregistered versions fall back to the nearest registered
// earlier version at lookup time (lenient; correct for the version-invariant
// block header). The only construction-time requirement is a NON-EMPTY registry
// (so DecoderFor can always return something). genesisVersion is per-network DATA
// (the version active from genesis_height), not a network branch.
func NewStaticRouter(upgrades []Upgrade, registry map[string]decoders.Decoder, genesisVersion string) (Router, error) {
	if len(registry) == 0 {
		return nil, fmt.Errorf("router: empty decoder registry")
	}
	sorted := append([]Upgrade(nil), upgrades...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].AppliedAtHeight < sorted[j].AppliedAtHeight })
	return &staticRouter{upgrades: sorted, registry: registry, genesisVersion: genesisVersion}, nil
}

// DecoderFor returns the decoder for the protocol version active at height,
// falling back to the nearest EARLIER registered version if the exact version's
// decoder is not yet implemented (LENIENT — correct for the version-invariant
// block header during incremental decoder rollout: the registry holds a
// representative subset of version decoders, while the upgrades table may name
// versions whose decoders arrive in later phases).
//
// This is PURELY version-based: the only per-network input is the upgrades data
// (which version is active at which height); the resolution logic never branches
// on network. The "version active at height" sequence is [genesis_version @
// genesis_height, then upgrades ascending]; we pick the latest entry <= height
// and walk back to the nearest registered version. If nothing at-or-before height
// is registered (e.g. a network whose genesis version we have not implemented),
// we fall back to the EARLIEST registered version — still correct for the
// version-invariant header, still network-agnostic.
//
// NOTE for later phases: version-SPECIFIC categories (tx/state/event) must NOT
// tolerate this fallback — when those land, the registry must cover every version
// in the table (Phase F), or DecoderFor must gain a strict variant.
func (r *staticRouter) DecoderFor(height int64) (decoders.Decoder, error) {
	// chosen tracks the nearest registered version at-or-before height, starting
	// from the genesis version (the height-0 entry) if it is registered.
	var chosen decoders.Decoder
	if d, ok := r.registry[r.genesisVersion]; ok {
		chosen = d
	}
	for _, u := range r.upgrades { // ascending by AppliedAtHeight
		if u.AppliedAtHeight > height {
			break
		}
		if rd, ok := r.registry[u.DecoderVersion]; ok {
			chosen = rd // a nearer registered version <= height
		}
		// unregistered intermediate version → keep the previous chosen (fallback)
	}
	if chosen != nil {
		return chosen, nil
	}
	// Nothing at-or-before height is registered: fall back to the earliest
	// registered version (version-invariant header → any decoder is correct).
	for _, u := range r.upgrades {
		if rd, ok := r.registry[u.DecoderVersion]; ok {
			return rd, nil
		}
	}
	return nil, fmt.Errorf("router: empty decoder registry (height %d)", height)
}
