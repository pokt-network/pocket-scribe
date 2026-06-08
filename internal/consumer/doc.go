// Package consumer is the generic consumer runtime: cursor tracking via
// consumer_consolidation, processed_heights writes, ack-after-commit
// pattern (invariant #5), passive gap detection, self-registration in
// consumer_registry, restart safety.
// See docs/superpowers/specs/2026-06-08-slice-1-design.md Section 4.6.
package consumer
