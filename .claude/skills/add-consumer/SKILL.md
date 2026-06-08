---
name: add-consumer
description: Scaffold a new consumer module (supplier, application, gateway, etc.) following PocketScribe patterns — generates the consumer Go code, SQL migration, tests, and CLI registration. Enforces TDD by generating tests first.
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add a new consumer module

Use this skill when adding indexing support for a new poktroll module (entity type). It generates all required artifacts in the right order, respecting the project's hard invariants.

## Inputs

Ask the user for:
1. **Module name** (lowercase, singular): `supplier`, `application`, `gateway`, `service`, `session`, `tokenomics`, `bank`, `authz`.
2. **Source store key** (matches poktroll `app.toml`): usually same as module name.
3. **NATS subject filter**: typically `pokt.kv.<module>.>` for KV-driven, `pokt.events.<event>.>` for event-driven.
4. **Is this an entity with lifecycle?** (Supplier yes; Claim no — claims are events.)
   - If yes → also need to design the `*_history` schema.
   - If no → only need event hypertables.

## Steps (execute in order)

### 1. Read context

- `CLAUDE.md`
- `docs/architecture/02-ingestion.md`
- `docs/architecture/03-data-model.md`
- Existing consumer at `internal/consumer/modules/` (pick the closest analog).

### 2. Spawn pocketscribe-schema-designer

If this is a new entity with lifecycle, ask the schema designer to design the `*_history` table and produce the migration. Wait for its output before proceeding.

### 3. Spawn pocketscribe-test-author for TDD scaffolding

Ask it to produce:
- Unit test for the decoder→canonical mapping.
- Component test for consumer→DB write (testcontainers).
- Golden test scaffold with empty fixture file.

### 4. Generate consumer code

Create files:

**`internal/consumer/modules/<module>/handler.go`**:
```go
package <module>

import (
    "context"
    "github.com/<org>/pocketscribe/internal/consumer"
    "github.com/<org>/pocketscribe/internal/router"
    "github.com/<org>/pocketscribe/internal/types"
    "github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct {
    db     *pgxpool.Pool
    router *router.Router
}

func New(db *pgxpool.Pool, r *router.Router) *Handler {
    return &Handler{db: db, router: r}
}

// ProcessKV is called for each StoreKVPair belonging to the <module> store.
func (h *Handler) ProcessKV(ctx context.Context, blockHeight int64, blockTime time.Time, kv types.KVChange) error {
    decoder := h.router.For(blockHeight)
    snapshot, err := decoder.Decode<Entity>KV(kv.Value)
    if err != nil {
        return fmt.Errorf("decode <module>: %w", err)
    }
    
    // upsert into <module>_history (append-only)
    return h.upsertSnapshot(ctx, blockHeight, blockTime, snapshot)
}

func (h *Handler) upsertSnapshot(ctx context.Context, height int64, t time.Time, s *types.<Entity>Snapshot) error {
    _, err := h.db.Exec(ctx, `
        INSERT INTO <module>_history (
            address, block_height, block_time,
            -- entity-specific fields
            ...
            snapshot_method, proto_version
        ) VALUES (
            $1, $2, $3, ..., $N
        ) ON CONFLICT (address, block_height) DO UPDATE SET
            -- updates only metadata that may differ (snapshot_method on reconciler corrections)
            snapshot_method = EXCLUDED.snapshot_method
    `, s.Address, height, t, ..., s.SnapshotMethod, s.ProtoVersion)
    return err
}
```

**`internal/consumer/modules/<module>/handler_test.go`**:
```go
package <module>

import (
    "testing"
    "github.com/stretchr/testify/require"
)

func TestHandler_ProcessKV_<Module>Snapshot_v0_1_5(t *testing.T) {
    // TDD: write the test first; implementation follows.
    t.Skip("TODO: implement after fixture is captured")
}
```

### 5. Register in dispatch

Edit `internal/consumer/dispatch.go`:
```go
import "github.com/<org>/pocketscribe/internal/consumer/modules/<module>"

func registerModules(...) {
    // existing registrations
    register("<module>", <module>.New(db, router))
}
```

### 6. Register CLI subcommand

Edit `internal/app/consumer/cmd.go`:
```go
var moduleNames = []string{
    "supplier", "application", "gateway", "service", "session",
    "tokenomics", "bank", "authz",
    "<module>",  // ADD
}
```

(Or add via init() if using dynamic registration.)

### 7. Update Tiltfile

Add a new Tilt resource:
```python
k8s_resource(
    'ps-consumer-<module>',
    port_forwards=[],
    labels=['consumers'],
)
```

### 8. Update docs

- `docs/architecture/03-data-model.md`: document the new entity.
- `CONTRIBUTING.md` mentions the module name in the example list.

### 9. Verify

Run:
```bash
make gen-check
make lint
make test
```

Report back to the user:
```
✅ Consumer scaffold for <module> complete.
✅ Schema migration: schema/migrations/NNNN_<module>_history.sql
✅ Handler: internal/consumer/modules/<module>/handler.go
✅ Test scaffold: internal/consumer/modules/<module>/handler_test.go
✅ Registered in dispatch + CLI
✅ Tiltfile updated
✅ Docs updated

Next steps:
1. Capture a golden fixture for the test (testnet/mainnet block with this event)
2. Implement the test logic (TDD: see it fail first)
3. Implement the handler logic to make it pass
4. Run /invariant-check before commit
```
