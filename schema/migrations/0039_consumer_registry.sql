-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS consumer_registry (
    consumer_name        TEXT PRIMARY KEY,
    first_valid_version  TEXT NOT NULL,        -- semver tag, e.g. "v0.1.0"
    active               BOOLEAN NOT NULL DEFAULT true,
    registered_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    deregistered_at      TIMESTAMPTZ
);
-- +goose StatementEnd
-- +goose StatementBegin
COMMENT ON TABLE consumer_registry IS
    'Self-registered consumer instances. Used to derive required_set(H) for per-height sealing. Consumers INSERT on startup (idempotent via ON CONFLICT DO NOTHING). Operators flip active=false via ps deregister-consumer for explicit decommission.';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS consumer_registry;
-- +goose StatementEnd
