-- +goose Up
-- +goose StatementBegin
-- ─────────────────────────────────────────────────────────────────────────────
-- COMMENT ON for all initial tables, views, and columns.
--
-- These comments drive auto-generated documentation in:
--   - Hasura GraphQL (shows them as schema descriptions in GraphiQL)
--   - PostgREST OpenAPI (shows them in the generated spec / Swagger UI)
--   - psql `\d <table>` and `\dD <type>`
--
-- DISCIPLINE: every new table / column added in subsequent migrations MUST
-- include COMMENT ON. The reviewer agent flags missing comments.
-- ─────────────────────────────────────────────────────────────────────────────

-- ═══ Coordination & metadata tables ═══

COMMENT ON TABLE block IS
$$Authoritative mapping from block_height to block_time (chain consensus header time).
Used as the Rosetta stone for converting between height and time axes in queries.
Every other table joins to this when needed.$$;

COMMENT ON COLUMN block.height IS 'Chain block height. PK.';
COMMENT ON COLUMN block.time IS
    'Consensus time from Tendermint header. NEVER indexer write time. Used for time_bucket() and time-range queries.';
COMMENT ON COLUMN block.hash IS 'Block hash (hex, lowercased).';
COMMENT ON COLUMN block.proposer_address IS 'Validator address of the block proposer.';
COMMENT ON COLUMN block.tx_count IS 'Number of transactions in the block.';

COMMENT ON TABLE processed_heights IS
$$Per-consumer per-height marker. Inserted in the same Postgres transaction as the
consumer's data writes. Used for gap detection and consumer_consolidation maintenance.$$;

COMMENT ON COLUMN processed_heights.consumer_name IS 'Name of the consumer (supplier, application, etc.).';
COMMENT ON COLUMN processed_heights.height IS 'Block height processed by this consumer.';
COMMENT ON COLUMN processed_heights.processed_at IS 'Indexer-side timestamp (audit only, NOT a query axis).';

COMMENT ON TABLE consumer_consolidation IS
$$Highest contiguous block_height each consumer has processed without gaps.
Used by sealing loop to decide which aggregate buckets can be sealed.$$;

COMMENT ON COLUMN consumer_consolidation.consumer_name IS 'Consumer identifier; PK.';
COMMENT ON COLUMN consumer_consolidation.consolidated_up_to IS 'Highest gap-free height. 0 = nothing processed.';

COMMENT ON TABLE aggregate_registry IS
$$Declarative catalog of continuous aggregates. The sealing loop iterates active
rows here and seals buckets when consumers_needed have caught up.

`status` lifecycle: shadow (materialized but not exposed) → public (exposed via APIs) → deprecated.$$;

COMMENT ON COLUMN aggregate_registry.name IS 'Aggregate name; matches the MATERIALIZED VIEW name. PK.';
COMMENT ON COLUMN aggregate_registry.description IS 'Human-readable description; surfaced in Hasura/PostgREST docs.';
COMMENT ON COLUMN aggregate_registry.source_tables IS 'Tables this aggregate reads from. Used by late-arrival invalidator.';
COMMENT ON COLUMN aggregate_registry.depends_on IS 'Other aggregates this depends on (for hierarchical aggregates).';
COMMENT ON COLUMN aggregate_registry.bucket_size IS 'Time bucket interval (e.g., 1 hour, 1 day).';
COMMENT ON COLUMN aggregate_registry.consumers_needed IS 'Consumers whose consolidated_up_to must >= bucket.height_last before sealing.';
COMMENT ON COLUMN aggregate_registry.status IS 'shadow = computed, not exposed. public = exposed via APIs. deprecated = scheduled for removal.';
COMMENT ON COLUMN aggregate_registry.sealed_strategy IS 'eager = seal as soon as possible. lazy = seal on first dirty event. manual = only on explicit refresh.';

COMMENT ON TABLE bucket_seal IS
$$Marks a continuous aggregate bucket as materialized and gap-free.
Downstream queries that require trustworthy aggregates JOIN with this table.$$;

COMMENT ON COLUMN bucket_seal.aggregate_name IS 'Aggregate this seal belongs to.';
COMMENT ON COLUMN bucket_seal.bucket_start_time IS 'Bucket start (block_time, not indexer time).';
COMMENT ON COLUMN bucket_seal.bucket_end_time IS 'Bucket end (exclusive).';
COMMENT ON COLUMN bucket_seal.height_range_first IS 'First chain block_height in this bucket (NULL = empty bucket = chain halt).';
COMMENT ON COLUMN bucket_seal.height_range_last IS 'Last chain block_height in this bucket.';
COMMENT ON COLUMN bucket_seal.sealed_at IS 'When the sealing loop confirmed this bucket. Updates on re-seal.';
COMMENT ON COLUMN bucket_seal.sealed_by_consumers IS 'Which consumers were caught up when this bucket sealed.';

COMMENT ON TABLE cagg_dirty_buckets IS
$$Invalidation queue. When a late arrival lands in a previously-sealed bucket,
the consumer enqueues an entry here. The sealing loop drains this and re-seals.$$;

COMMENT ON TABLE param_history IS
$$Module governance parameters (SCD2). Values change rarely via governance proposals.
THE documented exception to "no valid_to_height" — params benefit from materialized
ranges for ergonomics. Maintenance job is idempotent.$$;

COMMENT ON COLUMN param_history.module IS 'Cosmos SDK module key (e.g., supplier, tokenomics).';
COMMENT ON COLUMN param_history.name IS 'Parameter name within the module.';
COMMENT ON COLUMN param_history.value IS 'Parameter value as JSONB.';
COMMENT ON COLUMN param_history.effective_from_height IS 'Inclusive: from this height the value is in effect.';
COMMENT ON COLUMN param_history.effective_to_height IS 'Exclusive: NULL = still in effect.';

COMMENT ON TABLE upgrades IS
$$Chain upgrade ledger. Populated from poktroll x/upgrade module.
Used by internal/router to dispatch each block_height to the correct decoder version.$$;

COMMENT ON COLUMN upgrades.name IS 'Upgrade plan name from x/upgrade.';
COMMENT ON COLUMN upgrades.applied_at_height IS 'Block height where the upgrade took effect.';
COMMENT ON COLUMN upgrades.decoder_version IS 'Internal decoder version name (e.g., v0_1_5).';

-- ═══ Entity history tables ═══










-- ═══ Views ═══


COMMENT ON VIEW safe_height IS
$$The minimum consolidated_up_to across all consumers. Queries that require
cross-entity consistency should filter by block_height <= (SELECT height FROM safe_height).$$;

-- ═══ Event hypertables ═══







-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- COMMENT ON statements are documentation only; the down is a no-op.
-- (PostgreSQL doesn't support "uncomment"; the next "ALTER TABLE ... COMMENT" overwrites.)
SELECT 1;
-- +goose StatementEnd
