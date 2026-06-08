---
name: pre-commit-check
description: Full pre-commit gauntlet — runs gen-check, lint, unit tests, invariant audit. The skill version of `make ci` plus invariant checks.
allowed-tools: Bash, Read, Grep
---

# Pre-commit check

Run before every commit. Mirrors what CI does plus invariant audit.

## Steps

```bash
# 1. Ensure generated code is up-to-date
make gen-check                              # fails if regenerating would diff

# 2. Linter
make lint

# 3. Unit tests
make test

# 4. Format check
gofumpt -l ./... | tee /tmp/fmt.out
[ -s /tmp/fmt.out ] && { echo "Run 'make fmt' to fix formatting"; exit 1; }

# 5. Invariant audit
# (invoke the invariant-audit skill or inline)
git diff --cached schema/ | grep -E "valid_to_height" | grep -v "param_history" && echo "❌ invariant 2 violation" && exit 1
git diff --cached | grep -E "UPDATE \w+_history" && echo "❌ invariant 2 violation" && exit 1
# ... etc

# 6. Schema migration validation
LATEST_MIGRATION=$(ls schema/migrations | sort | tail -1)
echo "Latest migration: $LATEST_MIGRATION"
# verify monotonically numbered, no gaps

# 7. Generated code untouched
git diff --cached --name-only | grep -E "internal/(decoders/v[0-9_]+|store|proto)/gen/" && \
  echo "❌ generated code edited; run 'make gen' instead" && exit 1
```

If all pass:
```
✅ All pre-commit checks passed.
Commit when ready.
```

If any fail, report which step failed and the fix:
```
❌ Pre-commit check failed: lint
   → Run `make lint-fix` and re-run.
```

## Integration with the hook

`.claude/settings.json` already wires `make ci` into `Bash(git commit*)` PreToolUse hook. This skill adds the invariant audit on top, which is **not** in `make ci` (it's a static analysis, not a build/test step).

To enforce this skill on every commit, you can either:
1. Add it to `make ci` as a step (recommended).
2. Add it as a separate PreToolUse hook on git commit.

## Bypass

Sometimes you need to commit something failing (WIP commit, broken-test-on-purpose for TDD red phase). Use `git commit --no-verify` consciously. Document why in the commit message.
