-- +goose Up
-- +goose StatementBegin
-- ─────────────────────────────────────────────────────────────────────────────
-- Roles for PostgREST.
--
-- PostgREST authenticates as the role specified by JWT (or as db-anon-role
-- if no JWT). We grant SELECT on PocketScribe's public tables to a dedicated
-- anon role for unauthenticated reads.
--
-- Production deployments add additional roles per consumer with row-level
-- security policies if needed.
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
BEGIN
  -- Create role if missing (idempotent)
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pocketscribe_anon') THEN
    CREATE ROLE pocketscribe_anon NOLOGIN;
  END IF;

  -- Grant schema usage
  EXECUTE 'GRANT USAGE ON SCHEMA public TO pocketscribe_anon';

  -- Grant SELECT on all existing tables, views
  EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO pocketscribe_anon';

  -- Future tables also get SELECT automatically
  EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pocketscribe_anon';
  EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO pocketscribe_anon';
END
$$;

COMMENT ON ROLE pocketscribe_anon IS
    'Read-only role for PostgREST anonymous access. Used as db-anon-role in postgrest.conf.';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Revoke (idempotent)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pocketscribe_anon') THEN
    EXECUTE 'REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM pocketscribe_anon';
    EXECUTE 'REVOKE USAGE ON SCHEMA public FROM pocketscribe_anon';
    EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM pocketscribe_anon';
    EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON SEQUENCES FROM pocketscribe_anon';
    -- DROP ROLE pocketscribe_anon;  -- left in place; manual cleanup
  END IF;
END
$$;
-- +goose StatementEnd
