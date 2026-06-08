# Development Workflow

Day-to-day workflow for working on PocketScribe. Optimized for fast feedback, prod parity, and TDD discipline.

## Principles

- **TDD by default** — write the test, see it red, write the code, see it green.
- **DRY across the codebase** — but don't introduce abstractions for things that just look similar.
- **Prod parity in dev** — local stack mirrors production topology (Tilt + kind/k3d).
- **The Makefile is the contract** — anything CI does, you can do locally with one `make` command.

## Prerequisites

Install once:

| Tool | How |
|---|---|
| Go 1.26+ | https://go.dev/dl/ (or `gvm`/`asdf`) |
| Docker | https://docs.docker.com/engine/install/ |
| kind | `go install sigs.k8s.io/kind@latest` |
| kubectl | https://kubernetes.io/docs/tasks/tools/ |
| helm | https://helm.sh/docs/intro/install/ |
| Tilt | https://docs.tilt.dev/install.html |
| golangci-lint | https://golangci-lint.run/welcome/install/ |
| buf | https://buf.build/docs/installation |
| sqlc | https://docs.sqlc.dev/en/stable/overview/install.html |
| goose | `go install github.com/pressly/goose/v3/cmd/goose@latest` |
| mockery | `go install github.com/vektra/mockery/v3@latest` |

All of the above can be batch-installed with:

```bash
make install-deps
```

## Daily workflow

### 1. Start the stack

```bash
# First time:
make cluster-up                # creates kind cluster

# Every day:
tilt up                        # spins poktroll + NATS + Postgres + ps services
                               # opens UI at http://localhost:10350
```

The Tilt UI shows:
- Status of every service (poktroll, NATS, Postgres, ps subcommands)
- Logs (tailed live, color-coded per service)
- Resource health (CPU, memory)
- Hot-reload triggers (saves to `*.go` files rebuild and redeploy)

### 2. Make changes

- Edit Go files → Tilt detects → rebuild → redeploy → ready in seconds.
- Edit SQL migration → `tilt trigger db-migrate` (or auto-trigger on file save).
- Edit proto → `tilt trigger gen-proto` (regenerates Go types).

### 3. Run tests as you go

```bash
# Unit tests for the package you're editing (fast)
go test ./internal/consumer/modules/supplier/...

# All unit tests (still <10s)
make test

# Integration test (testcontainers spin up; ~30s)
make test-integration

# Full stack E2E (slow; only when you change interfaces or critical flows)
make test-e2e
```

### 4. Pre-commit hygiene

```bash
make ci                        # exactly what CI runs: tidy + gen-check + lint + test + test-integration
```

If `make ci` fails, **fix before pushing**. CI is not a debugging tool.

### 5. Commit

```bash
git add ...
git commit -m "Add supplier consumer dispatch logic (ADR-007)"
```

Pre-commit hooks (installed via `make install-hooks`):
- `gofumpt` formats
- `golangci-lint --fix` fixes safe issues
- `buf format` normalizes protos
- Blocks commit if `make gen-check` would fail

### 6. PR

```bash
gh pr create --title "..." --body "..."
```

CI runs the same `make ci` plus E2E suite. Reviewer uses the PR checklist in `CONTRIBUTING.md`.

## TDD loop in practice

### When adding a new feature

1. **Open the relevant test file** (or create one).
2. **Write the failing test first**:
   ```go
   func TestSupplierConsumer_HandlesEventSupplierStaked_v0_1_5(t *testing.T) {
       // Arrange: raw event bytes from golden fixture
       raw := goldens.Load(t, "v0_1_5/event_supplier_staked_001.bin")
       
       consumer := newTestConsumer(t)
       
       // Act
       err := consumer.processEvent(ctx, raw, blockHeight, blockTime)
       
       // Assert: expected row in supplier_history
       require.NoError(t, err)
       got := consumer.db.GetSupplierAt("pokt1...", blockHeight)
       require.Equal(t, "5000000000", got.StakeUpokt.String())
       require.Equal(t, blockHeight, got.BlockHeight)
       require.Equal(t, "EventSupplierStaked", got.TriggeredByEvent)
   }
   ```
3. **Run it. See red.** `go test -run TestSupplierConsumer_HandlesEventSupplierStaked_v0_1_5 ./internal/consumer/modules/supplier/...`
4. **Write the minimal code to make it green.**
5. **Run it. See green.**
6. **Refactor** if needed (now safe — tests guarantee correctness).
7. **Add edge cases**: malformed input, version boundaries, late-arrival ordering.
8. **Add component test** if the feature spans NATS↔DB (use testcontainers).
9. **Commit with the test included.**

### When fixing a bug

1. **Write the test that reproduces the bug.**
2. **See it red** — confirms the bug exists and is reproducible.
3. Fix the code.
4. **See it green.**
5. **Commit test + fix together.**

This ensures the bug never silently reappears.

## Tilt-specific tips

### Hot reload speed

Tilt builds inside the kind cluster using Docker. Build time per change ~2-5s. To speed up:

```python
# Tiltfile snippet
load('ext://restart_process', 'docker_build_with_restart')

docker_build_with_restart(
    'ps-consumer',
    context='.',
    dockerfile='deploy/docker/Dockerfile.dev',
    entrypoint=['/app/ps', 'consumer', 'supplier'],
    live_update=[
        sync('./internal', '/app/internal'),
        sync('./cmd', '/app/cmd'),
        run('cd /app && go build -o ps ./cmd/ps', trigger=['./internal', './cmd']),
        restart_container(),
    ],
)
```

`live_update` syncs files into the running container and rebuilds inside it — no image rebuild needed. <1s typical.

### Selective stack

`tilt up` brings everything. For lighter dev:

```bash
tilt up --resources poktroll,nats,postgres,fileplugin,consumer-supplier
```

Brings only what you need for testing the supplier consumer.

### Debugging

```bash
# Shell into a pod
kubectl exec -it deployment/ps-consumer-supplier -- /bin/sh

# Tail Postgres
kubectl exec -it statefulset/postgres -- psql -U pocketscribe

# Tail NATS
nats stream ls --server nats://localhost:4222
nats consumer report POKT --server nats://localhost:4222
```

(Tilt port-forwards these by default; check the Tilt UI for ports.)

## Common tasks

### Add a new consumer module

```bash
ps scaffold consumer <module_name>
# or via slash command in Claude Code:
/scaffold-consumer <module_name>
```

This generates:
- `internal/consumer/modules/<name>/`
- Migration in `schema/<NNNN>_<name>_history.sql`
- Test in `internal/consumer/modules/<name>/handler_test.go`
- Golden file directory `test/integration/fixtures/<name>/`

Then:
1. Write the test (TDD).
2. Implement the handler.
3. Register the module in `internal/consumer/dispatch.go`.
4. Add CLI registration in `internal/app/consumer/cmd.go`.
5. Update `Tiltfile` to launch `ps consumer <name>`.

### Add a new aggregate

```bash
/scaffold-aggregate <name> <bucket_size>
```

Generates:
- Migration with `CREATE MATERIALIZED VIEW`
- `INSERT INTO aggregate_registry` row
- Integration test seeding known data and asserting values

Initially `status = 'shadow'`. Promote to `'public'` only after spot-check validation against the chain.

### Onboard a new poktroll version

```bash
/generate-decoder <version_tag>
```

Clones poktroll at the tag, vendors protos to `third_party/proto/poktroll/v{X}_{Y}_{Z}/`, runs buf to generate Go types in `internal/decoders/v{X}_{Y}_{Z}/gen/`, creates a stub `decoder.go`. Then:

1. Compare protos: `buf breaking third_party/proto/poktroll/v{X}_{Y}_{Z} --against third_party/proto/poktroll/v{X}_{Y}_{Z-1}`
2. Document breaking changes in `docs/decisions/`.
3. Update `internal/router/upgrades.go` with the upgrade height (query mainnet `x/upgrade applied-plan`).
4. Add to CI matrix in `.github/workflows/proto-matrix.yml`.

### Replay a buggy module range

```bash
# Locally:
ps replay --module=supplier --from=100000 --to=200000

# In production:
kubectl exec -it deployment/ps-consumer-supplier -- ps replay --module=supplier --from=100000 --to=200000
```

The consumer pauses live ingestion, deletes the affected range, replays from NATS (within retention) or from chain gRPC (beyond retention), then resumes live.

## Anti-patterns

- ❌ Writing implementation before the test.
- ❌ Skipping `make ci` "because it's just a doc change" (CI catches accidental side effects).
- ❌ Editing generated code (`internal/store/gen/`, `internal/decoders/*/gen/`).
- ❌ Adding logic to `cmd/ps/main.go` (belongs in `internal/app/`).
- ❌ Adding `time.Now()` to a query (use `block_time`).
- ❌ Putting NATS subject names in a string literal (use `internal/nats/subjects.go`).
- ❌ Mocking the database in integration tests (use testcontainers).

## When things go wrong

| Symptom | Likely cause |
|---|---|
| `tilt up` shows poktroll OOMing | Increase memory limits in `Tiltfile`; reduce `keys` in app.toml to reduce streaming load |
| Consumer reports decoder error | Wrong proto version selected by router. Check `internal/router/upgrades.go`. |
| NATS Msg-Id collision warnings | Duplicate publish from two sidecars. Expected during HA testing; verify dedup is working. |
| Tests fail in CI but pass locally | Run `make gen-check` locally — likely stale generated code. |
| Postgres `lock_timeout` errors | Long-running compaction or reconciler holding rows. Check `pg_stat_activity`. |
| Compression policy errors during backfill | Compression disabled during backfill; enable only after stable: `SELECT add_compression_policy(...)`. |

See `docs/operations/runbook.md` for production troubleshooting.
