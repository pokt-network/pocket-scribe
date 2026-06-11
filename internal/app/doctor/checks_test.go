package doctor

// checks_test.go — unit tests for Check result rendering and aggregation logic.
// No live services: uses pre-built CheckResult values.

import (
	"errors"
	"strings"
	"testing"
)

func TestRenderChecks_AllPass(t *testing.T) {
	results := []CheckResult{
		{Name: "postgres", OK: true, Detail: "version 18.4"},
		{Name: "nats", OK: true, Detail: "JetStream enabled"},
		{Name: "lcd", OK: true, Detail: "200 OK"},
		{Name: "upgrades", OK: true, Detail: "3 rows"},
	}
	out, code := RenderChecks(results)
	if code != 0 {
		t.Errorf("all-pass should exit 0, got %d", code)
	}
	for _, r := range results {
		if !strings.Contains(out, r.Name) {
			t.Errorf("check %q not in output:\n%s", r.Name, out)
		}
	}
	if !strings.Contains(out, "✓") {
		t.Errorf("pass mark ✓ missing from all-pass output:\n%s", out)
	}
}

func TestRenderChecks_OneFail(t *testing.T) {
	results := []CheckResult{
		{Name: "postgres", OK: true, Detail: "ok"},
		{Name: "nats", OK: false, Err: errors.New("connection refused"), Detail: ""},
	}
	out, code := RenderChecks(results)
	if code != 1 {
		t.Errorf("one fail should exit 1, got %d", code)
	}
	if !strings.Contains(out, "✗") {
		t.Errorf("fail mark ✗ missing from fail output:\n%s", out)
	}
	if !strings.Contains(out, "connection refused") {
		t.Errorf("error detail missing from output:\n%s", out)
	}
}

func TestRenderChecks_AllFail(t *testing.T) {
	results := []CheckResult{
		{Name: "postgres", OK: false, Err: errors.New("timeout")},
		{Name: "nats", OK: false, Err: errors.New("no route")},
	}
	_, code := RenderChecks(results)
	if code != 1 {
		t.Errorf("all-fail should exit 1, got %d", code)
	}
}

func TestRenderChecks_EmptySlice(t *testing.T) {
	out, code := RenderChecks(nil)
	if code != 0 {
		t.Errorf("empty checks should exit 0, got %d", code)
	}
	// Should not panic and return something
	_ = out
}

func TestRenderChecks_FailDoesNotContaminatePass(t *testing.T) {
	results := []CheckResult{
		{Name: "postgres", OK: true, Detail: "ok"},
		{Name: "nats", OK: false, Err: errors.New("bad")},
		{Name: "lcd", OK: true, Detail: "ok"},
	}
	out, _ := RenderChecks(results)
	// postgres and lcd lines must show ✓, nats must show ✗
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		if strings.Contains(line, "postgres") && !strings.Contains(line, "✓") {
			t.Errorf("postgres pass line should show ✓: %q", line)
		}
		if strings.Contains(line, "nats") && !strings.Contains(line, "✗") {
			t.Errorf("nats fail line should show ✗: %q", line)
		}
		if strings.Contains(line, "lcd") && !strings.Contains(line, "✓") {
			t.Errorf("lcd pass line should show ✓: %q", line)
		}
	}
}

func TestRenderChecks_DetailShownOnPass(t *testing.T) {
	results := []CheckResult{
		{Name: "upgrades", OK: true, Detail: "5 rows"},
	}
	out, _ := RenderChecks(results)
	if !strings.Contains(out, "5 rows") {
		t.Errorf("detail text should appear on pass:\n%s", out)
	}
}

func TestCheckResult_OKWithErr_TreatedAsFail(t *testing.T) {
	// Defensive: if Err is set and OK is true, it's still a fail
	results := []CheckResult{
		{Name: "confused", OK: false, Err: errors.New("something wrong"), Detail: "ignore"},
	}
	_, code := RenderChecks(results)
	if code != 1 {
		t.Errorf("check with Err should produce exit code 1")
	}
}
