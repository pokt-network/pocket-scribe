// Package store wraps pgx v5 + sqlc-generated queries per ADR-016.
// All Postgres access flows through this package. Migrations live in
// schema/migrations/ and are applied via goose.
package store
