// Package sync is the composition root for ps sync-upgrades. Queries
// the configured chain RPC for applied_plan/{name} and upserts the
// upgrades table. See ADR-018; spec Section 4.4.
package sync
