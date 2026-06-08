---
description: Run `ps doctor` and interpret results — health check for Postgres, NATS, poktroll node, FilePlugin output.
---

Run the `ps doctor` health check:

```bash
ps doctor --json | tee /tmp/ps-doctor.json
```

Then interpret each check:

| Check | What it verifies | If fails |
|---|---|---|
| `postgres.connect` | Can connect to Postgres + Timescale extension installed | Check `DATABASE_URL`, run `psql` manually |
| `postgres.migrations` | All migrations applied | Run `ps migrate up` |
| `nats.connect` | Can connect to NATS cluster + JetStream | Check NATS pods, replicas, network |
| `nats.streams` | Required streams (POKT_CHAIN) exist with expected config | Run `make nats-streams` |
| `nats.consumers` | Each enabled consumer has a durable consumer registered | Restart the relevant `ps consumer` |
| `node.rpc` | poktroll RPC reachable | Check node pod, port-forward |
| `node.height` | Node not stalled (latest height advancing) | Check node logs, peers |
| `fileplugin.directory` | Output dir writable; recent files exist | Check app.toml streaming config; permissions |
| `fileplugin.sidecar` | Sidecar process is running and cursor advancing | Check `ps fileplugin` process / pod |
| `consumer.cursors` | Each consumer's consolidated_up_to within N blocks of head | Check consumer lag |
| `sealing.loop` | Last sealing pass <N seconds ago | Check sealing process |
| `reconciler.last_run` | Last reconciliation <N minutes ago | Check reconciler cron / process |

Report:
```
✅ All systems healthy.

Or:
⚠️  3 warnings:
  - sealing.loop: last pass 12 min ago (expected <1 min)
    → Check `ps sealing` is running
  - ...

🔴 1 critical:
  - postgres.migrations: 2 pending
    → Run `ps migrate up`
```
