-- +goose Up
-- +goose StatementBegin
-- Phase E (decision 5): from v0.1.8 the chain stores Supplier DEHYDRATED; the
-- hydrated service-config truth lives in ServiceConfigUpdate primary KV records
-- (ServiceConfigUpdate/service_id/...). They are first-class chain state —
-- append-only snapshots here; hydration happens at QUERY time (invariant 3).
-- See docs/research/phase-e-spike-findings.md §4d.
CREATE TABLE IF NOT EXISTS supplier_service_config_update_history (
  operator_address    TEXT        NOT NULL,
  service_id          TEXT        NOT NULL,
  activation_height   BIGINT      NOT NULL,
  deactivation_height BIGINT      NOT NULL DEFAULT 0,
  service_config      JSONB       NULL,
  deleted             BOOLEAN     NOT NULL DEFAULT FALSE,
  block_height        BIGINT      NOT NULL,
  block_time          TIMESTAMPTZ NOT NULL,
  decoded_by_version  SMALLINT    NOT NULL,
  indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT supplier_scu_history_pk PRIMARY KEY (operator_address, service_id, activation_height, block_height),
  CONSTRAINT supplier_scu_history_decoder_fk FOREIGN KEY (decoded_by_version) REFERENCES decoder_version(id)
);
COMMENT ON TABLE supplier_service_config_update_history IS
  'Append-only snapshots of ServiceConfigUpdate primary KV records (chain stores Supplier dehydrated from v0.1.8; ADR-005). deactivation_height 0 = none. deleted=TRUE records a chain KV deletion.';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS supplier_service_config_update_history;
-- +goose StatementEnd
