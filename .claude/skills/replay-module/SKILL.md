---
name: replay-module
description: Replay (reindex) a height range for one or more modules. Used after a decoder bug fix or reconciler drift detection. Wraps `ps replay` with safety checks.
allowed-tools: Read, Bash, Grep
---

# Replay a module height range

Use this skill when you've identified a height range with incorrect data (decoder bug, reconciler-detected drift, missing snapshots) and need to re-process it.

## Inputs

Ask the user for:
1. **Module** (e.g. `supplier`, `application`, `tokenomics`). One or more.
2. **From height** (start of affected range).
3. **To height** (end of affected range, or `head` for current).
4. **Why** (decoder bug, reconciler drift, schema migration, etc.) — for the commit message / audit log.

## Steps

### 1. Pre-flight checks

```bash
# Confirm the bug is fixed (test passes)
make test

# Confirm we have an up-to-date deploy (don't replay against a buggy consumer)
ps version

# Check consumer is healthy
ps doctor
```

If any check fails → STOP. Fix the underlying issue before replay.

### 2. Estimate scope

```bash
# How many blocks in the range?
psql -c "SELECT COUNT(*) FROM block WHERE height BETWEEN $FROM AND $TO"

# How many entity changes in this range for the module?
psql -c "SELECT COUNT(*) FROM <module>_history WHERE block_height BETWEEN $FROM AND $TO"

# How many events?
psql -c "SELECT COUNT(*) FROM event_<entity>_X WHERE block_height BETWEEN $FROM AND $TO"
```

If the range is huge (>10% of total), reconsider — maybe a full reconcile sweep is cheaper.

### 3. Take a backup checkpoint (optional but recommended)

For high-stakes replays:
```bash
psql -c "PG_DUMP <module>_history WHERE block_height BETWEEN $FROM AND $TO" > /backup/replay-checkpoint-$(date +%Y%m%dT%H%M%S).sql
```

### 4. Execute the replay

```bash
ps replay --module=$MODULE --from=$FROM --to=$TO

# OR for multiple modules
ps replay --module=supplier,application --from=$FROM --to=$TO
```

Internally, this:
1. Pauses live ingestion for the affected module (consumer ack-stops but doesn't crash).
2. Deletes rows in `<module>_history` WHERE block_height BETWEEN $FROM AND $TO.
3. Deletes affected event hypertable rows in the range.
4. Replays from NATS (if within retention) OR from gRPC archive (if beyond).
5. Resumes live ingestion.
6. Triggers `cagg_dirty_buckets` invalidation for affected aggregate buckets.

### 5. Verify

```bash
# Trigger sealing for affected buckets
ps sealing --once

# Run reconciler on the module
ps reconcile --module=$MODULE --at-height=$TO

# Should report 0 drifts
```

### 6. Audit log

Append to `docs/operations/replay-log.md` (or open a GitHub issue):
```markdown
- Date: 2026-05-22
- Module: supplier
- Range: 100000-110000
- Reason: Decoder bug in v0_1_5 (rev_share parsing); fixed in PR #42.
- Reconciler post-check: 0 drifts.
- Operator: <name>
```

### Output report

```
✅ Replay complete.

Module: supplier
Range: 100000-110000 (10001 blocks)
Rows deleted before replay: 1234
Rows re-inserted: 1234
Aggregate buckets invalidated + re-sealed: 24
Reconciler post-check: PASS (0 drifts)

Audit logged to docs/operations/replay-log.md.
```

## When NOT to use this skill

- For the entire chain history → use `ps backfill --from-genesis` (different procedure).
- For a single block → not worth the overhead; let reconciler auto-heal.
- During active outages → fix the outage first; replay later.

## Common scenarios

### Decoder bug discovered after deploy

1. Identify affected version range (from `upgrades` table — `block_height BETWEEN $UPGRADE_AT_H_FOR_BUGGY_VERSION AND $UPGRADE_AT_H_FOR_FIXED_VERSION`).
2. Deploy the fix.
3. `ps replay --module=<affected> --from=<H1> --to=<H2>`.

### Reconciler keeps detecting drift in one entity

1. Inspect the discrepancy: `psql ... vs poktrolld query`.
2. If the difference is meaningful (not a parsing edge case in goldens): fix the decoder.
3. Replay the entity's history range.

### Schema migration changed how a field is interpreted

1. After migration, old rows may have wrong values.
2. Replay all affected modules from the height where the new interpretation should kick in.
