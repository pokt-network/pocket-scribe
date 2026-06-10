//go:build integration

// Package testcontainers provides TimescaleDB + NATS JetStream container
// constructors for integration tests. Everything here is integration-tagged so
// it never compiles into the fast unit build. It is a pure constructor library;
// the package-shared singletons + TestMain live in test/integration.
package testcontainers

import (
	"context"
	"fmt"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// pgImage matches the dev stack and the verify-migrations skill.
const pgImage = "timescale/timescaledb:latest-pg18"

// PG bundles a running TimescaleDB container with a connected pool.
type PG struct {
	DSN       string
	Pool      *pgxpool.Pool
	Container *postgres.PostgresContainer
}

// StartPostgres launches a TimescaleDB container, applies every embedded
// migration, and returns a connected pool. extra customizers let the resilience
// test bind a fixed host port.
func StartPostgres(ctx context.Context, extra ...testcontainers.ContainerCustomizer) (*PG, error) {
	opts := make([]testcontainers.ContainerCustomizer, 0, 4+len(extra))
	opts = append(opts,
		postgres.WithDatabase("pocketscribe"),
		postgres.WithUsername("pocketscribe"),
		postgres.WithPassword("dev_only_password"),
		// The v0.40 postgres module applies NO wait strategy by default — without
		// this the container is reported "ready" before Postgres accepts
		// connections and goose fails. BasicWaitStrategies waits for the readiness
		// log (twice, due to the init restart) and the listening port.
		postgres.BasicWaitStrategies(),
	)
	opts = append(opts, extra...)

	c, err := postgres.Run(ctx, pgImage, opts...)
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("connection string: %w", err)
	}
	if err := store.Migrate(ctx, dsn, "up"); err != nil {
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	return &PG{DSN: dsn, Pool: pool, Container: c}, nil
}

// Reset truncates the Phase B coordination tables, Phase D data tables, and
// Phase E supplier tables so a test starts clean without re-applying migrations.
func (pg *PG) Reset(t *testing.T) {
	t.Helper()
	_, err := pg.Pool.Exec(context.Background(),
		`TRUNCATE consumer_registry, consumer_consolidation, processed_heights, block, upgrades,
		 msg_stake_supplier, msg_unstake_supplier,
		 event_supplier_staked, event_supplier_unbonding_begin, event_supplier_unbonding_canceled,
		 event_supplier_unbonding_end, event_supplier_service_config_activated,
		 supplier_history, supplier_service_config_update_history
		 RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("reset tables: %v", err)
	}
}

// PostgresFixedPort starts a dedicated TimescaleDB container bound to hostPort
// so it can be stopped and restarted at a stable address. Used only by the
// Postgres-restart resilience test. Terminated via t.Cleanup.
func PostgresFixedPort(t *testing.T, hostPort string) *PG {
	t.Helper()
	ctx := context.Background()
	pg, err := StartPostgres(ctx, fixedHostPort("5432/tcp", hostPort))
	if err != nil {
		t.Fatalf("start fixed-port postgres: %v", err)
	}
	t.Cleanup(func() {
		pg.Pool.Close()
		_ = pg.Container.Terminate(ctx)
	})
	return pg
}

// fixedHostPort binds containerPort (e.g. "5432/tcp") to a fixed hostPort so a
// container keeps the same address across stop/start.
func fixedHostPort(containerPort, hostPort string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.HostConfigModifier = func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				nat.Port(containerPort): []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: hostPort}},
			}
		}
		return nil
	}
}
