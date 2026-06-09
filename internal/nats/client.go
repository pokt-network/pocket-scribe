package nats

import (
	"context"
	"fmt"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client wraps a NATS connection and its JetStream context. It auto-reconnects
// indefinitely so a transient NATS outage does not kill a consumer.
type Client struct {
	nc *natsgo.Conn
	js jetstream.JetStream
}

// Connect dials url with infinite reconnect and returns a JetStream-ready
// client.
func Connect(ctx context.Context, url string) (*Client, error) {
	nc, err := natsgo.Connect(url,
		natsgo.RetryOnFailedConnect(true),
		natsgo.MaxReconnects(-1),
		natsgo.ReconnectWait(200*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("connect nats %s: %w", url, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	return &Client{nc: nc, js: js}, nil
}

// JetStream returns the JetStream context.
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// Conn returns the underlying NATS connection.
func (c *Client) Conn() *natsgo.Conn { return c.nc }

// Close drains and closes the connection.
func (c *Client) Close() { c.nc.Close() }

// EnsureStream creates or updates the POKT stream with a dedup window so that
// duplicate Nats-Msg-Ids within the window are dropped by the server.
func (c *Client) EnsureStream(ctx context.Context, dedupeWindow time.Duration) (jetstream.Stream, error) {
	return c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   StreamSubjects,
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: dedupeWindow,
	})
}
