// Package router resolves block_height -> decoder version via the
// upgrades table (DB-driven per ADR-018). No hardcoded heights.
// Periodic refresh keeps the in-memory map fresh as new upgrades land.
package router
