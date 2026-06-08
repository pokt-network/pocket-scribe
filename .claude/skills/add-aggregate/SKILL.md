---
name: add-aggregate
description: Scaffold a new continuous aggregate (rewards_hourly, claims_daily, etc.) following PocketScribe's registry-driven pattern with bucket sealing. Generates migration, registry entry, sealing test, and late-arrival test.
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add a new continuous aggregate

Use this skill when adding a new time-bucketed aggregate (hourly rewards, daily relays, etc.).

## Inputs

Ask the user for:
1. **Aggregate name** (snake_case): `rewards_hourly`, `claims_daily`, `relays_supplier_hourly`.
2. **Bucket size** (Postgres interval): `'1 hour'`, `'1 day'`, `'1 week'`, `'1 month'`.
3. **Source table(s)** (existing hypertable(s)): `event_claim_settled`, `event_proof_updated`.
4. **Dimension columns** (group-by axes besides time): `supplier_address`, `service_id`, etc.
5. **Aggregation columns** (`SUM`, `COUNT`, `AVG`): which fields and which functions.
6. **Consumers needed** (which consumers must be consolidated for sealing): `['tokenomics']` or `['supplier', 'tokenomics']`.

## Steps

### 1. Read context

- `CLAUDE.md`
- `docs/architecture/04-aggregates.md`
- Existing aggregates in `schema/migrations/`.

### 2. Spawn pocketscribe-aggregate-designer (if complex)

For aggregates that span multiple source tables or have hierarchical deps, delegate to the agent first.

### 3. Generate the migration

`schema/migrations/<NNNN>_<aggregate_name>.sql`:

```sql
-- +goose Up
-- +goose StatementBegin

-- Create the continuous aggregate
CREATE MATERIALIZED VIEW <aggregate_name>
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('<bucket_size>', block_time) AS bucket_start,
    <dimension_columns>,
    <aggregation_columns>,
    MIN(block_height) AS first_height_in_bucket,
    MAX(block_height) AS last_height_in_bucket,
    COUNT(*) AS row_count
FROM <source_table>
GROUP BY bucket_start, <dimension_columns>;

-- Enable real-time aggregation (queries auto-union materialized + live)
ALTER MATERIALIZED VIEW <aggregate_name>
SET (timescaledb.materialized_only = false);

-- Register in aggregate_registry (status = shadow until validated)
INSERT INTO aggregate_registry (
    name,
    description,
    source_tables,
    depends_on,
    bucket_size,
    consumers_needed,
    status,
    created_at,
    sealed_strategy
) VALUES (
    '<aggregate_name>',
    '<one-line description>',
    ARRAY['<source_table>'],
    ARRAY[]::text[],
    '<bucket_size>'::interval,
    ARRAY['<consumer1>'],
    'shadow',
    now(),
    'lazy'
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM aggregate_registry WHERE name = '<aggregate_name>';
DROP MATERIALIZED VIEW <aggregate_name>;
-- +goose StatementEnd
```

### 4. Generate integration tests

`test/integration/aggregate_<aggregate_name>_test.go`:

```go
//go:build integration

package integration

import (
    "testing"
    "time"
    "github.com/stretchr/testify/require"
)

func TestAggregate_<AggregateName>_SealsWhenConsumerCaughtUp(t *testing.T) {
    stack := setupTestStack(t)
    defer stack.Teardown()

    // Seed source data at known heights
    seedEvents(t, stack.DB, []eventFixture{
        {height: 100, time: t0.Add(0*time.Minute),  supplier: "pokt1A", settled: 1000},
        {height: 110, time: t0.Add(10*time.Minute), supplier: "pokt1A", settled: 2000},
        {height: 120, time: t0.Add(20*time.Minute), supplier: "pokt1B", settled: 500},
    })

    // Mark consumer consolidated up to height 120
    stack.DB.Exec(`UPDATE consumer_consolidation SET consolidated_up_to = 120 WHERE consumer_name = '<consumer>'`)

    // Run sealing loop once
    sealer := sealing.New(stack.DB)
    sealer.RunOnce(ctx)

    // Assert bucket sealed
    var sealed bool
    stack.DB.QueryRow(`SELECT EXISTS (
        SELECT 1 FROM bucket_seal
        WHERE aggregate_name = '<aggregate_name>'
        AND bucket_start_time = $1
    )`, t0.Truncate(time.Hour)).Scan(&sealed)
    require.True(t, sealed)

    // Assert aggregate values
    var totalSettled int64
    stack.DB.QueryRow(`SELECT total_settled FROM <aggregate_name>
                       WHERE bucket_start = $1 AND supplier_address = 'pokt1A'`,
        t0.Truncate(time.Hour)).Scan(&totalSettled)
    require.Equal(t, int64(3000), totalSettled)
}

func TestAggregate_<AggregateName>_LateArrival_ReSealsWithCorrectValue(t *testing.T) {
    stack := setupTestStack(t)
    defer stack.Teardown()

    // Seed initial data, seal
    seedEvents(t, stack.DB, /* ... */)
    stack.DB.Exec(`UPDATE consumer_consolidation SET consolidated_up_to = 120 WHERE consumer_name = '<consumer>'`)
    sealer := sealing.New(stack.DB)
    sealer.RunOnce(ctx)

    initialSealedAt := getSealedAt(t, stack.DB, "<aggregate_name>", t0.Truncate(time.Hour))

    // Late arrival: insert event with timestamp inside the already-sealed bucket
    stack.DB.Exec(`INSERT INTO event_claim_settled (...) VALUES (...)`,
        105, t0.Add(5*time.Minute), "pokt1A", 999)
    
    // Manually enqueue invalidation (in production this is done by the consumer)
    stack.DB.Exec(`INSERT INTO cagg_dirty_buckets ... VALUES ('<aggregate_name>', $1, $2, 'late_arrival')`,
        t0.Truncate(time.Hour), t0.Truncate(time.Hour).Add(time.Hour))

    // Run sealing again
    sealer.RunOnce(ctx)

    // Assert re-sealed
    newSealedAt := getSealedAt(t, stack.DB, "<aggregate_name>", t0.Truncate(time.Hour))
    require.True(t, newSealedAt.After(initialSealedAt))

    // Assert corrected value (now includes the late arrival)
    var totalSettled int64
    stack.DB.QueryRow(`SELECT total_settled FROM <aggregate_name>
                       WHERE bucket_start = $1 AND supplier_address = 'pokt1A'`,
        t0.Truncate(time.Hour)).Scan(&totalSettled)
    require.Equal(t, int64(3999), totalSettled)  // 1000 + 2000 + 999
}
```

### 5. Update docs

- `docs/architecture/04-aggregates.md`: add row to "Active aggregates" table.

### 6. Verify

```bash
make migrate-up
make test-integration -run Test<AggregateName>
```

### 7. Promotion procedure (post-merge)

Before promoting from `shadow` to `public`:
1. Let the aggregate run on production data for ~1 week.
2. Spot-check 5 random buckets: manually compute expected from raw data, compare with aggregate.
3. If exact match: `UPDATE aggregate_registry SET status = 'public' WHERE name = '<aggregate_name>'`.
4. Update Hasura/PostgREST configs to expose the new aggregate (typically just refresh metadata).

Report:
```
✅ Aggregate <name> scaffolded.
✅ Migration: schema/migrations/<NNNN>_<name>.sql (status=shadow)
✅ Integration tests: test/integration/aggregate_<name>_test.go
✅ Docs updated.

Run: make migrate-up && make test-integration

Promote to public after 1 week of validation.
```
