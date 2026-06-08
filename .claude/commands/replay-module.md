---
description: Replay a module's height range after a bug fix. Wraps the replay-module skill.
---

Invoke the `replay-module` skill.

Expected usage: `/replay-module <module> <from-height> <to-height>`

Example: `/replay-module supplier 100000 110000`

The skill performs pre-flight checks, runs `ps replay`, then triggers sealing + reconciler to verify the fix.
