# ADR-013: Single `ps` binary with cobra subcommands

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude

## Context

PocketScribe has multiple long-running roles (sidecar, consumers, reconciler, sealing) and admin commands (migrate, inspect, replay, doctor). Two options:

- **Single binary with subcommands** (`ps consumer supplier`, `ps fileplugin`, `ps doctor`).
- **Multiple binaries** (`ps-sidecar`, `ps-consumer`, `ps-reconciler`, etc.).

The user explicitly requested: "ps indexer, ps consumer supplier, ps consumer x, ps fileplugin".

## Decision

A **single `ps` binary** built from `cmd/ps`, with cobra-driven subcommands.

```
ps fileplugin
ps consumer <module>
ps consumer supplier
ps indexer                  # runs all enabled consumers in one process
ps reconciler
ps sealing
ps migrate up|down|status
ps inspect streams|cursors|seals
ps replay --module=X --from=H1 --to=H2
ps backfill --from-genesis
ps reconcile --module=X
ps doctor
ps version
```

`cmd/ps/main.go` is a thin dispatcher; logic lives in `internal/app/<subcommand>/`.

## Consequences

### Positive

- **One image, one binary, one config-loading pattern, one logging setup.**
- **Operators learn one CLI.** Faster onboarding.
- **Common subcommands** (config flags, --help, --version) are inherited automatically.
- **Container `ENTRYPOINT ["/app/ps"]` + per-deployment `CMD [...]`** is clean.
- **Easier local testing** — same binary you ship to production runs locally.
- **Smaller deploy surface** — one image to scan for CVEs, build, push.

### Negative

- **Container image slightly larger** because all subcommand code is in one binary. At PocketScribe scale, negligible (~50 MB image).
- **Build time slightly longer** — one binary rebuilds when any subcommand changes. Mitigated by Tilt's `live_update` (only re-syncs the binary, doesn't rebuild image).
- **Coupled release versioning** — all subcommands share `ps version`. Acceptable (typically you want them in lockstep anyway).

### Neutral

- Memory footprint per running pod doesn't change — Go strips unused code paths during runtime via its single-binary model (subcommands not chosen don't load).

## Alternatives considered

### Option A: Multiple binaries (`cmd/sidecar`, `cmd/consumer`, etc.)
- Pro: smaller per-binary image (in theory; in practice the savings are <30 MB).
- Pro: independent versioning per subcommand.
- Con: code duplication for config, logging, metrics, lifecycle.
- Con: more deploy artifacts, more security scanning.
- **Rejected**: user explicitly requested single binary.

### Option B: Single `ps`, but no subcommands (config-driven role)
- Pro: even simpler CLI.
- Con: operators must edit config to switch roles → bad ergonomics.
- **Rejected**: subcommands are the right ergonomic granularity.

## Implementation notes

- `cmd/ps/main.go` ≤30 lines. Pure cobra wiring.
- Each subcommand lives in `internal/app/<subcommand>/`:
  - `cmd.go` — cobra command registration (typically via `init()`).
  - `run.go` — the `Run(ctx, cfg)` entry point.
- Common machinery (config loading, signal handling, logging, metrics setup) lives in `internal/app/shared/`.
- Persistent flags (`--config`, `--log-level`, `--metrics`) are on the root command.
- Subcommand-specific flags live on the subcommand.

## CLI conventions

- Verbs for actions (`migrate`, `replay`, `backfill`, `reconcile`).
- Nouns for roles (`consumer`, `reconciler`, `sealing`).
- Hyphens for subcommands (`ps consumer supplier`); flags use `--double-dash`.
- `--help` works on every level.
- Exit codes: 0 on success; non-zero on failure with a meaningful message.

## References

- User request in session: "me gustaria poder realizar este desarrollo en plan 'vibe coding'... CLI, ps indexer, ps consumer supplier, ps consumer x, ps fileplugin".
- `docs/research/go-project-layout.md` — layout decisions.
- `cmd/ps/main.go` — the root command.
