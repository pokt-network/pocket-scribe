# internal/protover

Single boundary where protocol version strings are normalized and compared. Internal code never compares raw version strings — all comparisons flow through this package to ensure semver ordering (never lexicographic). Handles the two spellings present in the system: dotted form (`"v0.1.30"`, used in `upgrades.name` and `consumer_registry.first_valid_version`) and underscored decoder-dir form (`"v0_1_30"`, used in `network.genesis_decoder_version` and `upgrades.decoder_version`).

## Invariants honored

- **Semver comparison** — uses `golang.org/x/mod/semver`; garbage input is rejected at the boundary rather than silently comparing as lowest.
- **Single normalization point** — callers must not call `strings.Compare` or `semver.Compare` directly on raw version strings; always call `Normalize` first.

## Entry points

- `Normalize(s string) (string, error)` — accepts dotted or underscored spelling; returns canonical dotted form (`"vMAJOR.MINOR.PATCH"`).
- `Compare(a, b string) int` — orders two normalized versions; inputs must come from `Normalize`.
- `ToDecoderDir(s string) (string, error)` — converts any accepted spelling to the underscored decoder-directory form.

## Testing

- **Unit** — `internal/protover/protover_test.go` covers both input spellings, invalid inputs, comparison ordering, and round-trips through `ToDecoderDir`.
