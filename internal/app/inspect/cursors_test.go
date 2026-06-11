package inspect

// cursors_test.go — unit tests for cursor row formatting logic.
// No DB required: tests use the CursorRow struct and RenderCursors directly.

import (
	"strings"
	"testing"
)

func TestRenderCursors_EmptySlice(t *testing.T) {
	out := RenderCursors(nil)
	if !strings.Contains(out, "CONSUMER") {
		t.Errorf("expected header row in empty output, got:\n%s", out)
	}
}

func TestRenderCursors_SingleRow(t *testing.T) {
	rows := []CursorRow{
		{
			Name:              "supplier",
			FirstValidVersion: "v0.1.0",
			Active:            true,
			ConsolidatedUpTo:  42000,
			ProcessedCount:    42001,
			LastHeight:        42000,
		},
	}
	out := RenderCursors(rows)
	if !strings.Contains(out, "supplier") {
		t.Errorf("consumer name missing from output:\n%s", out)
	}
	if !strings.Contains(out, "v0.1.0") {
		t.Errorf("first_valid_version missing from output:\n%s", out)
	}
	if !strings.Contains(out, "42000") {
		t.Errorf("consolidated_up_to missing from output:\n%s", out)
	}
	if !strings.Contains(out, "yes") {
		t.Errorf("active=true should render as 'yes':\n%s", out)
	}
}

func TestRenderCursors_InactiveConsumer(t *testing.T) {
	rows := []CursorRow{
		{
			Name:   "old-consumer",
			Active: false,
		},
	}
	out := RenderCursors(rows)
	if !strings.Contains(out, "no") {
		t.Errorf("active=false should render as 'no':\n%s", out)
	}
}

func TestRenderCursors_ZeroConsolidation(t *testing.T) {
	rows := []CursorRow{
		{
			Name:             "block",
			Active:           true,
			ConsolidatedUpTo: 0,
			ProcessedCount:   0,
			LastHeight:       0,
		},
	}
	out := RenderCursors(rows)
	if !strings.Contains(out, "block") {
		t.Errorf("consumer name missing:\n%s", out)
	}
	// Zero values should render as "0" not blank/error
	if strings.Contains(out, "Error") || strings.Contains(out, "panic") {
		t.Errorf("unexpected error text in output:\n%s", out)
	}
}

func TestRenderCursors_MultipleRows_Sorted(t *testing.T) {
	rows := []CursorRow{
		{Name: "supplier", Active: true, ConsolidatedUpTo: 100},
		{Name: "block", Active: true, ConsolidatedUpTo: 200},
		{Name: "application", Active: false, ConsolidatedUpTo: 50},
	}
	out := RenderCursors(rows)
	// All names must appear
	for _, name := range []string{"supplier", "block", "application"} {
		if !strings.Contains(out, name) {
			t.Errorf("name %q missing from output:\n%s", name, out)
		}
	}
}

func TestRenderCursors_HeaderFields(t *testing.T) {
	out := RenderCursors(nil)
	for _, col := range []string{"CONSUMER", "VERSION", "ACTIVE", "CONSOLIDATED", "PROCESSED", "LAST"} {
		if !strings.Contains(out, col) {
			t.Errorf("header column %q missing from output:\n%s", col, out)
		}
	}
}
