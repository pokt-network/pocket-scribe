---
description: Force a one-shot reconciliation pass. Wraps the reconcile-now skill.
---

Invoke the `reconcile-now` skill.

Usage:
- `/reconcile-now` — dry-run all modules at head - safety margin.
- `/reconcile-now <module>` — dry-run one module.
- `/reconcile-now <module> --heal` — auto-heal mode.

The skill reports per-module drift status and recommends next actions.
