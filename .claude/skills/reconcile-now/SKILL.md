---
name: reconcile-now
description: Trigger a one-shot reconciliation pass against the chain. Detects drift between indexed state and chain truth without waiting for the periodic loop.
allowed-tools: Read, Bash, Grep
---

# Reconcile now

Force a one-shot reconciliation. Useful after a deploy, after a partial backfill, or when investigating a suspected drift.

## Inputs

Ask the user (or default sensibly):
1. **Module** — specific module name, or `all` for everything.
2. **At-height** — chain height to reconcile against. Default: `head - 10` (safety margin).
3. **Mode** — `dry-run` (report only) or `auto-heal` (apply corrections). Default: `dry-run`.

## Steps

### 1. Pre-flight

```bash
# Chain reachable?
poktrolld status --node=tcp://<rpc-endpoint>

# DB reachable?
psql -c "SELECT 1"
```

### 2. Execute

```bash
# Dry-run on a single module
ps reconcile --module=$MODULE --at-height=$HEIGHT --dry-run

# Dry-run all
ps reconcile --all --dry-run

# Auto-heal a specific module
ps reconcile --module=$MODULE --at-height=$HEIGHT

# Force-heal even if mismatch count exceeds threshold
ps reconcile --module=$MODULE --at-height=$HEIGHT --force-heal
```

### 3. Interpret output

The CLI prints a per-module report:

```
Reconcile report (height=487231)

Module: supplier
  Chain entities:        6243
  Indexed entities:      6243
  Mismatches:            0
  Status: ✅ CLEAN

Module: application
  Chain entities:        148
  Indexed entities:      148
  Mismatches:            2
  Status: ⚠️  DRIFT DETECTED
  Mismatches:
    - pokt1abc...: stake mismatch (chain=10000000, indexed=9999000)
    - pokt1def...: missing service config
  Mode: dry-run (no corrections inserted)

Module: tokenomics
  Mismatches:            0
  Status: ✅ CLEAN
```

### 4. Investigate drift (if any)

For each mismatch:

```bash
# Pull both sides
psql -c "SELECT * FROM application_history WHERE address='pokt1abc...' ORDER BY block_height DESC LIMIT 5"
poktrolld query application show-application pokt1abc... --height=$HEIGHT --output json
```

Compare manually. Common causes:
- Decoder bug (some field parsed wrong) → fix decoder, replay range.
- Consumer missed an event → check `processed_heights` for gaps.
- Reconciler false positive (e.g., comparing serialized JSON with different field ordering) → fix Equal() in the reconciler.

### 5. Apply correction

If the drift is real and not many:
```bash
ps reconcile --module=$MODULE --at-height=$HEIGHT
# (without --dry-run, auto-heal applies if mismatches < threshold)
```

If many drifts (systemic bug):
- Don't auto-heal; that just papers over the issue.
- Fix the root cause (decoder bug, consumer logic, etc.).
- Run `ps replay --module=$MODULE --from=<H1> --to=<H2>` to clean up the range.
- Re-run `ps reconcile` to verify.

### Output report

```
✅ Reconciliation complete.

Mode: dry-run | auto-heal | force-heal
Modules checked: supplier, application, gateway, ...
Clean: 7
Drift detected: 1 (application, 2 mismatches)
Auto-healed: 0 (dry-run) | 2 (auto-heal)

[If drift detected:]
Next steps:
1. Investigate the 2 application mismatches.
2. If decoder bug: fix, then `ps replay --module=application --from=... --to=...`.
3. If transient (e.g., race): re-run reconcile in 1 minute.
```

## When to use

- After deploying a new decoder version.
- After a partial backfill.
- Periodically (in addition to the scheduled reconciler).
- During incident investigation.
- Before a major migration to confirm baseline integrity.

## When NOT to use

- During heavy live traffic — bulk chain queries add load. Use during low-traffic windows for large modules.
- If chain RPC is unreachable — wait for the chain to recover first.
- For a single entity — query both sources directly with psql + poktrolld; no need to run a full reconcile.
