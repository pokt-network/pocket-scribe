//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pokt-network/pocketscribe/internal/fileplugin"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
)

func TestBootstrapPublishesMetaFiles(t *testing.T) {
	stream := freshStream(t)

	dir := t.TempDir()
	heights := []int64{10, 20, 30}
	payloads := map[int64][]byte{
		10: []byte("meta-bytes-10"),
		20: []byte("meta-bytes-20"),
		30: []byte("meta-bytes-30"),
	}
	for _, h := range heights {
		name := filepath.Join(dir, "block-"+strconv.FormatInt(h, 10)+"-meta")
		if err := os.WriteFile(name, payloads[h], 0o644); err != nil {
			t.Fatalf("write fixture %d: %v", h, err)
		}
	}

	ctx := context.Background()
	n, err := fileplugin.Bootstrap(ctx, nats.Client, dir, 0)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if n != 3 {
		t.Fatalf("Bootstrap returned %d, want 3", n)
	}

	// Assert each message lands on the correct subject with correct bytes and MsgID.
	for _, h := range heights {
		subj := natsx.BlockSubject(h)
		wantID := natsx.MsgID(subj, h, 0)

		var gotData []byte
		var gotID string
		deadline := time.After(5 * time.Second)
		tick := time.NewTicker(50 * time.Millisecond)
	outer:
		for {
			m, err := stream.GetLastMsgForSubject(ctx, subj)
			if err == nil {
				gotData = m.Data
				gotID = m.Header.Get("Nats-Msg-Id")
				tick.Stop()
				break outer
			}
			select {
			case <-deadline:
				tick.Stop()
				t.Fatalf("no message on %s within timeout: %v", subj, err)
			case <-tick.C:
			}
		}

		if string(gotData) != string(payloads[h]) {
			t.Fatalf("height %d: payload = %q, want %q", h, gotData, payloads[h])
		}
		if gotID != wantID {
			t.Fatalf("height %d: Nats-Msg-Id = %q, want %q", h, gotID, wantID)
		}
	}
}

func TestBootstrapRespectsMaxHeight(t *testing.T) {
	freshStream(t)

	dir := t.TempDir()
	for _, h := range []int64{1, 5, 10, 15} {
		name := filepath.Join(dir, "block-"+strconv.FormatInt(h, 10)+"-meta")
		if err := os.WriteFile(name, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	n, err := fileplugin.Bootstrap(ctx, nats.Client, dir, 10)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if n != 3 { // heights 1, 5, 10 — 15 is skipped (maxHeight=10)
		t.Fatalf("Bootstrap returned %d, want 3 (max-height=10)", n)
	}
}
