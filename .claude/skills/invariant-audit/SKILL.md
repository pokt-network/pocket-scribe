---
name: invariant-audit
description: Audit recent changes (staged, uncommitted, or last N commits) for violations of PocketScribe's hard invariants. Fast guardrail before commit.
allowed-tools: Read, Grep, Glob, Bash
---

# Invariant audit

Scan changes for violations of the 5 hard invariants + banned patterns.

## What it checks

### Invariant 1: `(block_height, block_time)` everywhere

```bash
# Any new CREATE TABLE that doesn't have both columns?
git diff --cached schema/ | grep -A20 'CREATE TABLE' | grep -v 'block_height\|block_time'

# Any time_bucket using a non-block_time column?
git diff --cached schema/ | grep -E "time_bucket\([^,]+,\s*(now\(\)|clock_timestamp|indexed_at)"
```

### Invariant 2: Append-only history

```bash
# UPDATE statements on history tables (banned)
git diff --cached | grep -E "UPDATE \w+_history"

# valid_to_height column on a history table (banned)
git diff --cached schema/ | grep -E "valid_to_height|valid_to_time" | grep -v "param_history"
```

### Invariant 3: No event-derived state mutation

```bash
# Patterns like stake = stake + something
git diff --cached | grep -E "(stake|balance|supply)\s*=\s*\1\s*[+\-]"
```

### Invariant 4: Idempotent inserts

```bash
# New INSERT statements without ON CONFLICT
git diff --cached | grep -E "^\+\s*INSERT INTO" | grep -v "ON CONFLICT"
```

### Invariant 5: Ack after commit

```bash
# Ack before commit pattern (this is a Go heuristic; manual review may be needed)
git diff --cached --name-only | xargs grep -l "Ack()" | xargs grep -B5 "Ack()" | grep -E "Ack\(\)\s*$" | head -20
```

### Banned tools

```bash
# ClickHouse imports
git diff --cached --name-only | xargs grep -l "clickhouse"

# GORM
git diff --cached --name-only | xargs grep -l "gorm.io/gorm"

# LISTEN/NOTIFY
git diff --cached | grep -iE "LISTEN |NOTIFY "
```

### Generated code edits

```bash
# Manual edits to generated dirs (banned)
git diff --cached --name-only | grep -E "internal/(decoders/v[0-9_]+|store|proto)/gen/"
```

## Output format

```
## Invariant Audit Report
Scope: <staged | uncommitted | last 3 commits>

### 🔴 BLOCKING violations
- [Invariant 2] schema/migrations/0042_X.sql adds valid_to_height to supplier_history (line 12)
  → REMOVE this column. Use LEAD() at query time.

### 🟡 WARNINGS
- [Invariant 4] internal/consumer/modules/X/handler.go INSERT without ON CONFLICT (line 87)
  → Add ON CONFLICT (id) DO UPDATE/NOTHING.

### 🟢 NITS
- ...

### ✅ Clean
- Invariant 1: all new tables have block_height + block_time
- Invariant 5: ack-after-commit pattern preserved
- No banned tools introduced
- No edits to generated code

### Action
- [Fix violations above before committing.]
- [Or, if clean:] Safe to commit.
```

## When to invoke

- Before every commit (pre-commit hook can auto-run).
- During code review (audit a PR's changes).
- After a refactor (audit a range of commits).

## False positives

If a finding is a false positive (e.g., `valid_to_height` is in `param_history` which is documented exception), acknowledge it and skip:

```
- [Invariant 2] schema/0001_init.sql contains valid_to_height
  → False positive: param_history is the documented exception. SKIPPED.
```
