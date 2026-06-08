# Research

Focused technical research notes that informed PocketScribe's design.

## Public research (committed)

- [`file-plugin-spec.md`](./file-plugin-spec.md) — Cosmos SDK ABCI FilePlugin specification for v0.53.0 (formats, config, gotchas).
- [`go-project-layout.md`](./go-project-layout.md) — Survey of Go project layouts and the chosen structure for PocketScribe.

## Adding research

If you're researching something general (libraries, patterns, comparison studies), add a Markdown file here. Naming: `<topic>.md` or `<topic>-spec.md`.

If you're researching something **personal / exploratory / sensitive** that you don't want on GitHub:
- Drop it in [`personal/`](./personal/) (gitignored).
- Promote to public when ready.

See [`personal/README.md`](./personal/README.md) for details.

## What belongs in research vs decisions vs architecture

| Location | Purpose |
|---|---|
| `docs/research/` | Background investigation. "What's the state of X?" "How does Y work?" Inputs to a decision. |
| `docs/decisions/` | The actual decision. ADR format: context, decision, consequences. |
| `docs/architecture/` | The current state. "How we do X today." Living documentation. |

When something matures from research → decision → architecture, you typically:
1. Spend time in `docs/research/personal/` exploring.
2. Write a clean `docs/research/<topic>.md` summarizing findings.
3. Write `docs/decisions/ADR-NNN.md` choosing among findings.
4. Update `docs/architecture/` to reflect the new state.

Each layer survives. Research isn't deleted when a decision is made; it's the receipt.
