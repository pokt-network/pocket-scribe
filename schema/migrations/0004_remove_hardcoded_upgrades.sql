-- +goose Up
-- +goose StatementBegin
-- ─────────────────────────────────────────────────────────────────────────────
-- Previously this migration hardcoded the Shannon mainnet upgrade history.
-- That was a mistake: it tied PocketScribe to mainnet only and drifted from
-- the chain (see ADR-018-no-hardcoded-upgrades.md).
--
-- The `upgrades` table is now populated by `ps sync-upgrades` (which queries
-- the connected chain's `/cosmos/upgrade/v1beta1/applied_plan/{name}` endpoint
-- and the network's genesis info).
--
-- This migration is a no-op (kept for migration-numbering stability).
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 1; -- intentional no-op
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 1;
-- +goose StatementEnd
