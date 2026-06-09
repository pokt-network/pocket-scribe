package upgrades

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// fixtureEntry is one entry in test/fixtures/sync-upgrades/mainnet-applied-plans.json.
type fixtureEntry struct {
	AppliedPlan struct {
		Height string `json:"height"`
	} `json:"applied_plan"`
	BlockTime string `json:"block_time"` // empty when height==0
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
