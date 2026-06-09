//go:build integration

package testcontainers

import (
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	natsx "github.com/pokt-network/pocketscribe/internal/nats"
)

// natsImage pins a JetStream-capable server. The testcontainers nats module
// starts it with `-js` (JetStream enabled) by default in v0.40.
const natsImage = "nats:2.10-alpine"

// NC bundles a running NATS JetStream container with a connected client.
type NC struct {
	URL       string
	Client    *natsx.Client
	Container *tcnats.NATSContainer
}

// StartNATS launches a NATS JetStream server and returns a connected client.
// extra customizers let the resilience test bind a fixed host port.
func StartNATS(ctx context.Context, extra ...testcontainers.ContainerCustomizer) (*NC, error) {
	c, err := tcnats.Run(ctx, natsImage, extra...)
	if err != nil {
		return nil, fmt.Errorf("start nats: %w", err)
	}
	url, err := c.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("nats connection string: %w", err)
	}
	client, err := natsx.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	return &NC{URL: url, Client: client, Container: c}, nil
}

// NATSFixedPort starts a dedicated NATS JetStream container bound to hostPort so
// it can be stopped and restarted at a stable address (NATS-reconnect test).
// Terminated via t.Cleanup.
func NATSFixedPort(t *testing.T, hostPort string) *NC {
	t.Helper()
	ctx := context.Background()
	nc, err := StartNATS(ctx, fixedHostPort("4222/tcp", hostPort))
	if err != nil {
		t.Fatalf("start fixed-port nats: %v", err)
	}
	t.Cleanup(func() {
		nc.Client.Close()
		_ = nc.Container.Terminate(ctx)
	})
	return nc
}
