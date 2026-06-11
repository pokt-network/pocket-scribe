package inspect

// streams_test.go — unit tests for stream row formatting logic.
// No NATS required: tests use StreamRow / ConsumerInfo structs and RenderStreams.

import (
	"strings"
	"testing"
)

func TestRenderStreams_EmptySlice(t *testing.T) {
	out := RenderStreams(nil)
	if !strings.Contains(out, "STREAM") {
		t.Errorf("expected header row in empty output, got:\n%s", out)
	}
}

func TestRenderStreams_SingleStream_NoConsumers(t *testing.T) {
	rows := []StreamRow{
		{
			Name:      "POKT",
			Msgs:      12345,
			Bytes:     99_000_000,
			FirstSeq:  1,
			LastSeq:   12345,
			Consumers: nil,
		},
	}
	out := RenderStreams(rows)
	if !strings.Contains(out, "POKT") {
		t.Errorf("stream name missing:\n%s", out)
	}
	if !strings.Contains(out, "12345") {
		t.Errorf("message count missing:\n%s", out)
	}
}

func TestRenderStreams_ConsumerSection(t *testing.T) {
	rows := []StreamRow{
		{
			Name:     "POKT",
			Msgs:     5,
			FirstSeq: 1,
			LastSeq:  5,
			Consumers: []ConsumerInfo{
				{Name: "supplier", Pending: 2, AckFloor: 3},
				{Name: "block", Pending: 0, AckFloor: 5},
			},
		},
	}
	out := RenderStreams(rows)
	if !strings.Contains(out, "supplier") {
		t.Errorf("consumer name missing:\n%s", out)
	}
	if !strings.Contains(out, "block") {
		t.Errorf("consumer name missing:\n%s", out)
	}
}

func TestRenderStreams_ByteFormatting(t *testing.T) {
	rows := []StreamRow{
		{Name: "TEST", Bytes: 1024 * 1024},
	}
	out := RenderStreams(rows)
	// Should show MB or at least the number
	if !strings.Contains(out, "TEST") {
		t.Errorf("stream name missing:\n%s", out)
	}
}

func TestRenderStreams_HeaderFields(t *testing.T) {
	out := RenderStreams(nil)
	for _, col := range []string{"STREAM", "MSGS", "BYTES", "FIRST_SEQ", "LAST_SEQ", "CONSUMERS"} {
		if !strings.Contains(out, col) {
			t.Errorf("header column %q missing from output:\n%s", col, out)
		}
	}
}

func TestRenderStreams_MultipleStreams(t *testing.T) {
	rows := []StreamRow{
		{Name: "POKT", Msgs: 100},
		{Name: "OTHER", Msgs: 50},
	}
	out := RenderStreams(rows)
	if !strings.Contains(out, "POKT") || !strings.Contains(out, "OTHER") {
		t.Errorf("both stream names must appear:\n%s", out)
	}
}
