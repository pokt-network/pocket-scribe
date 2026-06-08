# Working with Claude on PocketScribe

> How to "vibe code" PocketScribe with Claude Code without losing context, drifting from invariants, or accumulating tech debt. This is the operator's manual for human-Claude collaboration.

## The core problem

A long-running project like PocketScribe has months of context: 12 ADRs, 89 entities, 6 decoder versions over its lifetime, dozens of aggregates, runbooks, edge cases learned the hard way. Claude doesn't carry that across sessions. The risk:

- Forgets invariants → suggests `UPDATE supplier_history SET valid_to = ...` (banned).
- Forgets stack choices → suggests "let's use Redis for caching".
- Forgets prior decisions → re-debates settled ADRs.
- Drifts off-task in deep edits → "while we're here, let me refactor..." → context bloat.

The remedy is **structured persistence + scoped invocation**: the project ships with CLAUDE.md (always loaded), per-domain rules (loaded when relevant), specialized agents (focused subprompts), skills (reusable procedures), commands (one-line invocations), and settings hooks (deterministic enforcement).

## The 6-layer Claude integration

```
┌──────────────────────────────────────────────────────────────┐
│ Layer 1: CLAUDE.md (root)                                    │
│ Always loaded. Invariants, vocabulary, stack, banned things. │
│ ~400 lines. Optimized for token efficiency.                  │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ Layer 2: Path-scoped CLAUDE.md (per subdir, optional)        │
│ E.g. internal/decoders/CLAUDE.md with proto-specific rules.  │
│ Loaded on-demand when you edit files in that path.           │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ Layer 3: .claude/agents/                                     │
│ Specialized subagents with frontmatter + instructions.       │
│ Spawned explicitly when you need their expertise.            │
│ Each has its own context window — doesn't pollute main.      │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ Layer 4: .claude/skills/                                     │
│ Multi-step procedures. Loaded inline when invoked.           │
│ Examples: add-consumer, add-aggregate, invariant-audit.      │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ Layer 5: .claude/commands/                                   │
│ Slash commands that wrap skills with one-line invocation.    │
│ E.g. /scaffold-consumer, /generate-decoder.                  │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ Layer 6: .claude/settings.json                               │
│ Hooks for deterministic enforcement (pre-commit lints,       │
│ pre-tool-use checks). Hooks are non-negotiable.              │
└──────────────────────────────────────────────────────────────┘
```

## Recommended plugins / MCP servers

| Plugin | What it gives you | When to use |
|---|---|---|
| **context7** (already installed) | Fresh library docs (Go stdlib, Cosmos SDK, NATS, Timescale, pgx, cobra, buf) | Any library question. Always prefer this over your training data. |
| **pocket-network** (already installed) | Direct chain queries: balance lookups, validator info, block details, RPC inspection | Verifying reconciler logic, debugging entity drift, validating golden fixtures. |
| **kubernetes** (already installed) | kubectl/helm operations | Debugging Tilt deploys, inspecting pods, checking ingress. |
| **GitHub MCP** (install if not present) | PR creation, issue triage, comment management | PR workflow, issue references in commits. |
| **PostgreSQL MCP** (install if available) | Schema introspection, query execution | Debugging migrations, exploring `aggregate_registry`. |
| **filesystem MCP** | Local file operations beyond Read/Write | Rarely needed; built-in tools suffice. |

Install GitHub MCP if missing:
```bash
claude mcp add github
```

## How to invoke Claude at each stage

### Designing a feature → use `/gsd-discuss-phase` then `/gsd-plan-phase`

GSD (Get Sh*t Done) is a skill suite that enforces structured planning. For PocketScribe:

1. `/gsd-discuss-phase` — Claude asks targeted questions about gray areas before writing a plan. Avoids assumption-driven design.
2. `/gsd-plan-phase` — produces a PLAN.md with tasks, dependencies, and verification criteria.
3. `/gsd-execute-phase` — atomic execution with checkpoints; Claude can be paused/resumed cleanly.

For PocketScribe, every architectural change should go through this flow. Quick fixes can skip to `/gsd-quick`.

### Adding code → invoke a specialized agent or skill

| You want to... | Use |
|---|---|
| Add a new consumer module | `/scaffold-consumer <name>` (wraps the skill) |
| Add a new aggregate | `/scaffold-aggregate <name> <bucket>` |
| Onboard a poktroll version | `/generate-decoder <version_tag>` |
| Audit recent changes for invariant violations | `/invariant-check` |
| Bring dev stack online with health checks | `/tilt-up` |
| Discuss an architectural change | Spawn `pocketscribe-architect` agent |
| Design a new entity schema | Spawn `pocketscribe-schema-designer` agent |
| Write tests for a feature | Spawn `pocketscribe-test-author` agent |
| Review a PR for violations | Spawn `pocketscribe-reviewer` agent |

### Resuming after a break → `/gsd-resume-work` or `/gsd-progress`

These skills read the project state (open todos, current phase, recent commits) and produce a focused resume context. They prevent "where was I?" drift.

### Daily start of session

Run these in order to load focused context:

```
1. /gsd-progress              # see what's pending
2. /gsd-resume-work           # load context from last session
3. Open the file you're editing — path-scoped CLAUDE.md auto-loads
```

## Context preservation strategy

### Memory system (auto-loaded)

`MEMORY.md` (in user home) carries cross-session memory: facts about you, feedback you've given me, project context. **Already populated from our design session.** When you start a new session, this is what Claude knows about PocketScribe even before reading the repo.

Check what's loaded:
```
/memory list
```

Add manually:
```
/remember <fact>
```

Forget:
```
/forget <key>
```

### Project memory (in repo)

These files are always in scope:
- `CLAUDE.md` — invariants, stack, vocabulary
- `ROADMAP.md` — phased plan
- `docs/decisions/*.md` — ADRs (read on demand)

When you start a session: "I'm working on phase 1 spike. Read ROADMAP.md and CLAUDE.md if you haven't."

### Conversation continuation

Claude Code conversations persist locally. Resume any past conversation:
```
claude --resume
```

For PocketScribe, prefer **one conversation per phase** (or per feature). When the conversation gets long (>50 turns or context warning), `/gsd-pause-work` produces a handoff document, and you start a fresh session resuming from it.

## The "scope creep" prevention pattern

The single biggest risk in vibe-coding: Claude (or you!) drifting off-task. Three counter-measures:

1. **Tasks are explicit.** Use `TaskCreate` at session start. The progress shows what's pending — Claude won't wander unless tasks are vague.
2. **Hooks block side effects.** Pre-tool-use hook on `Edit` outside the current task's scope prompts "this is outside scope — confirm?".
3. **TodoWrite vs editing.** Default to TodoWrite for "we should also do X" — captures the idea without context switch.

## What's in `.claude/` (commit this)

```
.claude/
├── settings.json                       # hooks, permissions
├── agents/                             # specialized subagents
│   ├── pocketscribe-architect.md
│   ├── pocketscribe-schema-designer.md
│   ├── pocketscribe-proto-versioner.md
│   ├── pocketscribe-aggregate-designer.md
│   ├── pocketscribe-test-author.md
│   └── pocketscribe-reviewer.md
├── skills/                             # multi-step procedures
│   ├── add-consumer/SKILL.md
│   ├── add-aggregate/SKILL.md
│   ├── add-decoder-version/SKILL.md
│   ├── invariant-audit/SKILL.md
│   ├── pre-commit-check/SKILL.md
│   ├── replay-module/SKILL.md
│   └── reconcile-now/SKILL.md
├── commands/                           # slash command wrappers
│   ├── scaffold-consumer.md
│   ├── scaffold-aggregate.md
│   ├── generate-decoder.md
│   ├── invariant-check.md
│   ├── tilt-up.md
│   └── ps-doctor.md
└── rules/                              # path-scoped rules (Claude Code 2026+)
    ├── decoders.md                     # paths: internal/decoders/**
    ├── migrations.md                   # paths: schema/**
    ├── consumers.md                    # paths: internal/consumer/**
    └── proto.md                        # paths: **/*.proto
```

**Commit `.claude/` to git.** Skills and agents are project infrastructure, not personal config. (Personal config goes in `.claude/settings.local.json` which IS gitignored.)

## When to use which abstraction (decision flowchart)

```
You have a recurring task to automate.
   │
   ├── Is it a SINGLE deterministic action you want enforced?
   │     → Hook in settings.json (e.g. gofumpt pre-commit)
   │
   ├── Is it a multi-STEP procedure you'll run often?
   │     → Skill in .claude/skills/
   │     → Optional: wrap in slash command in .claude/commands/
   │
   ├── Is it a SPECIALIZED REASONING task (architecture, review)?
   │     → Agent in .claude/agents/
   │
   ├── Is it CONTEXT that should always be loaded?
   │     → CLAUDE.md (root or subdirectory)
   │
   └── Is it CONTEXT loaded only when editing certain paths?
         → .claude/rules/ with `paths:` frontmatter
```

## Anti-patterns to avoid

- ❌ **Putting "remember to do X every time" in CLAUDE.md.** Memory in CLAUDE.md is fragile; Claude may not always follow it. Use a **hook** for deterministic enforcement.
- ❌ **Bloating CLAUDE.md past 500 lines.** Token cost. Split into path-scoped rules.
- ❌ **One mega-agent that does everything.** Defeats the focused-context benefit. Many small agents > one giant one.
- ❌ **Skills that duplicate slash commands.** Pick one per task — skills are richer (frontmatter, supporting files); commands are pure aliases.
- ❌ **Editing `.claude/settings.local.json` and expecting team to inherit.** That's gitignored. Use `settings.json` for team-wide.
- ❌ **Skipping `/gsd-discuss-phase` for architectural changes.** Discussion catches gray areas before they're locked in code.

## Recommended session patterns

### Pattern A: Daily standup with Claude

```
Morning:
  /gsd-progress
  → Claude lists pending tasks across phases
You:
  "Let's work on task #5 — add the supplier consumer scaffold"
Claude:
  Uses /scaffold-consumer supplier
  Implements TDD loop
  Runs make ci
  Commits

Evening:
  /gsd-pause-work
  → Claude writes a handoff doc with state, next steps
```

### Pattern B: Architectural change

```
You:
  "I want to add a new aggregate that combines rewards across services."
Claude:
  /gsd-discuss-phase
  Asks: bucket size? consumers needed? shadow first or direct public?
You:
  Answer questions
Claude:
  /gsd-plan-phase
  Produces PLAN.md
You:
  Review, approve
Claude:
  /gsd-execute-phase
  Atomic execution with verification
```

### Pattern C: Bug fix

```
You:
  "Supplier with address X has wrong stake at height 145000"
Claude:
  Spawn pocketscribe-test-author agent
  Writes test reproducing the bug at height 145000
  Sees test fail
  Fixes the mapping
  Sees test pass
  /scaffold-replay for the affected height range
  Reconciler validates green
Claude:
  Commits test + fix
```

### Pattern D: Code review

```
You:
  Open PR
Claude:
  Spawn pocketscribe-reviewer agent
  Checks: invariants, test coverage, schema compatibility, ADR alignment
  Reports findings as PR comments via gh CLI
You:
  Address findings
```

## Settings hooks (what's in settings.json)

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash(git commit*)",
        "hooks": [
          {
            "type": "command",
            "command": "make ci"
          }
        ]
      },
      {
        "matcher": "Edit",
        "matcherPaths": ["internal/decoders/v*/gen/**", "internal/store/gen/**"],
        "hooks": [
          {
            "type": "command",
            "command": "echo 'BLOCKED: generated code, do not edit directly. Run make gen-{proto|sql}.'; exit 1"
          }
        ]
      }
    ],
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "echo 'PocketScribe session started. Run /gsd-progress to see status.'"
          }
        ]
      }
    ]
  },
  "permissions": {
    "allow": [
      "Bash(make *)",
      "Bash(go test*)",
      "Bash(go build*)",
      "Bash(tilt *)",
      "Bash(kubectl *)",
      "Bash(nats *)",
      "Bash(psql *)",
      "Bash(goose *)",
      "Bash(buf *)",
      "Bash(sqlc *)",
      "Bash(gh *)"
    ]
  }
}
```

This minimizes permission prompts for routine commands while keeping destructive operations gated.

## Mental model: Claude as your pair-programming partner

PocketScribe is designed for sustained collaboration with Claude:

- Claude knows the invariants (CLAUDE.md).
- Claude has specialized voices (agents).
- Claude has muscle memory (skills, commands).
- Claude follows the process (GSD, hooks).
- Claude doesn't drift (tasks, scoped edits).

Your job is to **frame the work** (open task, define scope) and **review outputs** (PR checklist). Claude's job is to **execute within the rails** the project provides.

When something feels off — "Claude is suggesting something that doesn't match our pattern" — that's a signal:
1. Either the suggestion is wrong → push back, cite the invariant.
2. Or the project is missing context → update CLAUDE.md or add a skill/agent.

The repo is alive. Don't accept friction silently; encode the lesson.
