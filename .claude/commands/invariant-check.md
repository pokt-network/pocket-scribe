---
description: Run a full invariant audit on staged changes (or uncommitted, or last N commits).
---

Invoke the `invariant-audit` skill.

Default scope: staged changes (`git diff --cached`).

Optional argument: `/invariant-check uncommitted` or `/invariant-check last-3`

Report findings using the format defined in the skill:
- 🔴 BLOCKING violations (must fix)
- 🟡 WARNINGS (should address)
- 🟢 NITS (optional)
- ✅ Clean items confirmed

If clean, recommend safe to commit. If violations, list specific fixes.
