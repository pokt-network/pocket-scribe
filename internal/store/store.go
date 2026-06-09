package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the single entry point for all Postgres access (ADR-016). It owns a
// pgx connection pool; every query in the indexer goes through this package.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a pgx pool against dsn (libpq keyword or URL form) and verifies
// connectivity with a ping.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for advanced callers (tests, bulk copy).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }
