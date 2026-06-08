---
description: Onboard a new poktroll version — vendor protos, codegen, scaffold decoder. Wraps the proto-versioner agent.
---

Spawn the `pocketscribe-proto-versioner` agent to onboard a new poktroll version.

Expected argument: `/generate-decoder <version_tag>`

Example: `/generate-decoder v0.1.6`

The agent will:
1. Clone poktroll at the tag
2. Vendor protos to `third_party/proto/poktroll/v{X}_{Y}_{Z}/`
3. Run `buf generate` to produce Go types
4. Create stub decoder
5. Compare protos with `buf breaking`
6. Update router upgrade table
7. Add cross-version test
8. Update CI matrix
9. Update docs

Walk through each step interactively, asking for confirmation when needed.
