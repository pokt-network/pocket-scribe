package upgrades

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
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
	// Serve applied_plan endpoints.
	for name, entry := range fixture {
		mux.HandleFunc("/cosmos/upgrade/v1beta1/applied_plan/"+name, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"height": entry.AppliedPlan.Height})
		})
		// Serve block endpoint only for applied entries (height != "0").
		if entry.AppliedPlan.Height != "0" && entry.AppliedPlan.Height != "" {
			mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/"+entry.AppliedPlan.Height, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				resp := map[string]any{
					"block": map[string]any{
						"header": map[string]string{
							"time": entry.BlockTime,
						},
					},
				}
				_ = json.NewEncoder(w).Encode(resp)
			})
		}
	}

	srv := httptest.NewServer(mux)
	defer srv.Close()

	got, err := New(srv.URL, srv.Client()).Fetch(context.Background(), []string{"v0.1.30", "v0.1.32"})
	if err != nil {
		t.Fatal(err)
	}
	// v0.1.32 is height 0 → skipped; v0.1.30 → height 484473, decoder v0_1_30
	if len(got) != 1 {
		t.Fatalf("got %d upgrades, want 1", len(got))
	}
	if got[0].AppliedAtHeight != 484473 || got[0].DecoderVersion != "v0_1_30" {
		t.Fatalf("unexpected: %+v", got[0])
	}
}
