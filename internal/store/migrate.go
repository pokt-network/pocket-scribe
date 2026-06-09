package store

import (
	"context"
	"database/sql"
	"fmt"

	// Register the pgx stdlib driver under the name "pgx" for goose, which
	// requires a *sql.DB. This is the migration-tool boundary only — all
	// runtime data access still goes through pgxpool above (ADR-016 honored;
	// database/sql is not used for query work).
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/pokt-network/pocketscribe/schema"
)

// Migrate applies the embedded migration set against dsn. command is one of
// "up", "down", "status". It opens its own short-lived *sql.DB because goose
// operates on database/sql, not pgxpool.
func Migrate(ctx context.Context, dsn, command string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open sql db for goose: %w", err)
	}
	defer func() { _ = db.Close() }()

	goose.SetBaseFS(schema.Migrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}

	const dir = "migrations" // relative to the embed root in schema.Migrations
	switch command {
	case "up":
		return goose.UpContext(ctx, db, dir)
	case "down":
		return goose.DownContext(ctx, db, dir)
	case "status":
		return goose.StatusContext(ctx, db, dir)
	default:
		return fmt.Errorf("unknown migrate command %q (want up|down|status)", command)
	}
}
