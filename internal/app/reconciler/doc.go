// Package reconciler is the composition root for ps reconciler.
// In Slice 1 it ships only the upgrades refresh loop (calls sync-upgrades
// periodically). Full drift detection is Slice 4.
// See docs/superpowers/specs/2026-06-08-slice-1-design.md Section 4.5.
package reconciler
