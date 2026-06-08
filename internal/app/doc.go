// Package app holds the composition roots for each ps subcommand.
//
// Each subpackage (fileplugin, consumer, indexer, reconciler, migrate,
// inspect, sync) owns its own Cobra command construction, dependency
// wiring, and lifecycle. cmd/ps/main.go is intentionally thin.
package app
