package upgrades

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pokt-network/pocketscribe/internal/fixturereport"
)

// fixtureEntry is one entry in test/fixtures/sync-upgrades/mainnet-applied-plans.json.
type fixtureEntry struct {
	AppliedPlan struct {
		Height string `json:"height"`
	} `json:"applied_plan"`
	BlockTime string `json:"block_time"` // empty when height==0
}

// TestNewDefaultClient verifies that New() uses http.DefaultClient when nil is passed.
func TestNewDefaultClient(t *testing.T) {
	s := New("https://example.com", nil)
	if s == nil {
		t.Fatal("New returned nil")
	}
	// Verify trailing slash is trimmed.
	s2 := New("https://example.com/", nil)
	if s2.lcd != "https://example.com" {
		t.Fatalf("lcd = %q, want trailing slash trimmed", s2.lcd)
	}
}

// TestFetchHTTPError verifies that an HTTP error from the LCD is surfaced.
func TestFetchHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := New(srv.URL, srv.Client()).Fetch(context.Background(), []string{"v0.1.30"})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("error should mention status 500: %v", err)
	}
}

// TestFetchBadHeightJSON verifies error when height is not a valid integer.
func TestFetchBadHeightJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/upgrade/v1beta1/applied_plan/v0.1.30", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"height": "not-a-number"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New(srv.URL, srv.Client()).Fetch(context.Background(), []string{"v0.1.30"})
	if err == nil {
		t.Fatal("expected error for non-numeric height")
	}
	if !strings.Contains(err.Error(), "parse height") {
		t.Fatalf("error should mention 'parse height': %v", err)
	}
}

// TestFetchBlockHTTPError verifies error when the block-time endpoint fails.
func TestFetchBlockHTTPError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/upgrade/v1beta1/applied_plan/v0.1.30", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"height": "100"})
	})
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/100", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New(srv.URL, srv.Client()).Fetch(context.Background(), []string{"v0.1.30"})
	if err == nil {
		t.Fatal("expected error when block endpoint returns 404")
	}
	if !strings.Contains(err.Error(), "status 404") {
		t.Fatalf("error should mention status 404: %v", err)
	}
}

// TestVersionToDecoderMapping verifies the name→decoder-version conversion.
func TestVersionToDecoderMapping(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{"v0.1.30", "v0_1_30"},
		{"v0.1.8", "v0_1_8"},
		{"v0.1.27", "v0_1_27"},
	}
	for _, c := range cases {
		if got := versionToDecoder(c.name); got != c.want {
			t.Errorf("versionToDecoder(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestFetchBadJSONBody verifies error when the response body is not valid JSON.
func TestFetchBadJSONBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/upgrade/v1beta1/applied_plan/v0.1.30", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{invalid json`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New(srv.URL, srv.Client()).Fetch(context.Background(), []string{"v0.1.30"})
	if err == nil {
		t.Fatal("expected error for invalid JSON response body")
	}
	if !strings.Contains(err.Error(), "decode") {
		t.Fatalf("error should mention 'decode': %v", err)
	}
}

// errorBodyTransport returns a response with a body that always errors on Read.
// This exercises the io.ReadAll error branch in getJSON (line 65-68 of upgrades.go).
type errorBodyTransport struct{}

func (errorBodyTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(&alwaysErrorReader{}),
		Header:     make(http.Header),
	}, nil
}

type alwaysErrorReader struct{}

func (*alwaysErrorReader) Read(_ []byte) (int, error) {
	return 0, errors.New("injected read error")
}

// TestFetchBodyReadError verifies that a body read failure in getJSON is surfaced.
func TestFetchBodyReadError(t *testing.T) {
	client := &http.Client{Transport: errorBodyTransport{}}
	_, err := New("http://unused", client).Fetch(context.Background(), []string{"v0.1.30"})
	if err == nil {
		t.Fatal("expected error when body read fails")
	}
	if !strings.Contains(err.Error(), "read") {
		t.Fatalf("error should mention 'read': %v", err)
	}
}

// TestFetchAppliedPlans exercises Fetch against an httptest server that replays
// the full mainnet golden fixture (all upgrade_names from mainnet.yaml, including
// the two never-applied ones: v0.1.1 and v0.1.32 with height "0").
//
// Cross-check: heights returned by Fetch must match fixturereport.MainnetUpgrades()
// exactly (chain-authoritative table). Any mismatch is a bug in the golden or in
// MainnetUpgrades.
func TestFetchAppliedPlans(t *testing.T) {
	// Load the golden fixture.
	raw, err := os.ReadFile("../../test/fixtures/sync-upgrades/mainnet-applied-plans.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture map[string]fixtureEntry
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}

	mux := http.NewServeMux()
	for name, entry := range fixture {
		mux.HandleFunc("/cosmos/upgrade/v1beta1/applied_plan/"+name, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"height": entry.AppliedPlan.Height})
		})
		if entry.AppliedPlan.Height != "0" && entry.AppliedPlan.Height != "" {
			mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/"+entry.AppliedPlan.Height, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"block": map[string]any{
						"header": map[string]string{"time": entry.BlockTime},
					},
				})
			})
		}
	}

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Build the full name list from the fixture (deterministic order).
	allNames := make([]string, 0, len(fixture))
	for name := range fixture {
		allNames = append(allNames, name)
	}
	sort.Strings(allNames)

	got, err := New(srv.URL, srv.Client()).Fetch(context.Background(), allNames)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// Build authoritative map from fixturereport (height + decoder).
	authoritative := fixturereport.MainnetUpgrades()
	authMap := make(map[string]struct {
		height  int64
		decoder string
	}, len(authoritative))
	for _, u := range authoritative {
		authMap[u.Name] = struct {
			height  int64
			decoder string
		}{u.AppliedAtHeight, u.DecoderVersion}
	}

	// v0.1.1 and v0.1.32 are height=0; they must be absent from got.
	gotMap := make(map[string]int64, len(got))
	for _, u := range got {
		gotMap[u.Name] = u.AppliedAtHeight
	}
	for _, skip := range []string{"v0.1.1", "v0.1.32"} {
		if _, present := gotMap[skip]; present {
			t.Errorf("expected %s (height=0) to be skipped but it appeared in Fetch result", skip)
		}
	}

	// Every applied entry in the fixture must appear in got with correct height + decoder.
	for name, entry := range fixture {
		if entry.AppliedPlan.Height == "0" || entry.AppliedPlan.Height == "" {
			continue
		}
		auth, inAuth := authMap[name]

		u, inGot := gotMap[name]
		if !inGot {
			t.Errorf("%s: present in fixture (height=%s) but missing from Fetch result", name, entry.AppliedPlan.Height)
			continue
		}

		// Cross-check: height must match MainnetUpgrades (if the name is registered there).
		if inAuth && u != auth.height {
			t.Errorf("%s: Fetch height=%d but MainnetUpgrades=%d — DISCREPANCY", name, u, auth.height)
		}
		if !inAuth {
			// v0.1.34 is in mainnet.yaml but not (yet) in MainnetUpgrades — flag but don't fail.
			t.Logf("NOTE: %s applied at height %d is in the fixture but not in MainnetUpgrades() — update fixturereport/mainnet.go", name, u)
		}
	}

	// Every entry in MainnetUpgrades must appear in got (the fixture must be complete).
	for _, auth := range authoritative {
		u, ok := gotMap[auth.Name]
		if !ok {
			t.Errorf("%s: in MainnetUpgrades (height=%d) but missing from Fetch result", auth.Name, auth.AppliedAtHeight)
			continue
		}
		if u != auth.AppliedAtHeight {
			t.Errorf("%s: Fetch height=%d, MainnetUpgrades=%d — DISCREPANCY", auth.Name, u, auth.AppliedAtHeight)
		}
	}

	// Spot-check decoder versions for a sample.
	for _, u := range got {
		want := versionToDecoder(u.Name)
		if u.DecoderVersion != want {
			t.Errorf("%s: DecoderVersion=%q, want %q", u.Name, u.DecoderVersion, want)
		}
	}

	// Spot-check block times are non-zero for all applied entries.
	for _, u := range got {
		if u.AppliedAtTime.IsZero() {
			t.Errorf("%s: AppliedAtTime is zero", u.Name)
		}
		if u.AppliedAtTime.Location() != time.UTC {
			t.Errorf("%s: AppliedAtTime is not UTC: %v", u.Name, u.AppliedAtTime)
		}
	}
}
