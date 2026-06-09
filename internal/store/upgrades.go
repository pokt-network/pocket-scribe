package store

import (
	"context"
	"fmt"
	"time"
)

// Upgrade is one row of the upgrades table (height → decoder version).
type Upgrade struct {
	Name            string
	AppliedAtHeight int64
	AppliedAtTime   time.Time
	DecoderVersion  string
}

// UpsertUpgrade idempotently records an applied upgrade. Keyed by name (PK);
// re-running sync-upgrades produces the same rows (idempotency invariant 4).
func (s *Store) UpsertUpgrade(ctx context.Context, u Upgrade) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO upgrades (name, applied_at_height, applied_at_time, decoder_version)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (name) DO UPDATE SET
		     applied_at_height = EXCLUDED.applied_at_height,
		     applied_at_time   = EXCLUDED.applied_at_time,
		     decoder_version   = EXCLUDED.decoder_version`,
		u.Name, u.AppliedAtHeight, u.AppliedAtTime, u.DecoderVersion)
	if err != nil {
		return fmt.Errorf("upsert upgrade %s: %w", u.Name, err)
	}
	return nil
}

// ListUpgrades returns all upgrades ordered by applied_at_height ASC (for the
// router's height→version mapping).
func (s *Store) ListUpgrades(ctx context.Context) ([]Upgrade, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, applied_at_height, applied_at_time, decoder_version
		 FROM upgrades ORDER BY applied_at_height ASC`)
	if err != nil {
		return nil, fmt.Errorf("list upgrades: %w", err)
	}
	defer rows.Close()
	var out []Upgrade
	for rows.Next() {
		var u Upgrade
		if err := rows.Scan(&u.Name, &u.AppliedAtHeight, &u.AppliedAtTime, &u.DecoderVersion); err != nil {
			return nil, fmt.Errorf("scan upgrade: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
