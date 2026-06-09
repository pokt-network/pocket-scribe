// Package upgrades discovers applied chain upgrades from the LCD and records
// them in the upgrades table (ADR-018). It is the engine behind
// `ps sync-upgrades` and the reconciler's periodic refresh.
package upgrades

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// HTTPDoer is the minimal HTTP surface sync needs; tests inject an httptest-backed client.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Syncer queries an LCD base URL for applied upgrade plans + their block times.
type Syncer struct {
	lcd    string // base URL, e.g. https://sauron-api.infra.pocket.network
	client HTTPDoer
}

// New builds a Syncer. If client is nil, http.DefaultClient is used.
func New(lcd string, client HTTPDoer) *Syncer {
	if client == nil {
		client = http.DefaultClient
	}
	return &Syncer{lcd: strings.TrimRight(lcd, "/"), client: client}
}

// appliedPlanResp is the LCD shape: {"height":"<N>"} ("0" = not applied).
type appliedPlanResp struct {
	Height string `json:"height"`
}

// blockResp is the LCD /cosmos/base/tendermint/v1beta1/blocks/{h} shape (subset).
type blockResp struct {
	Block struct {
		Header struct {
			Time time.Time `json:"time"`
		} `json:"header"`
	} `json:"block"`
}

func (s *Syncer) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.lcd+path, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// versionToDecoder maps an upgrade plan name "v0.1.30" to a decoder version dir "v0_1_30".
func versionToDecoder(name string) string { return strings.ReplaceAll(name, ".", "_") }

// Fetch queries each upgrade name; for applied ones (height>0) it fetches the
// block time and returns the slice of Upgrade rows. Pure HTTP → structs, no DB.
func (s *Syncer) Fetch(ctx context.Context, names []string) ([]store.Upgrade, error) {
	var out []store.Upgrade
	for _, name := range names {
		var plan appliedPlanResp
		if err := s.getJSON(ctx, "/cosmos/upgrade/v1beta1/applied_plan/"+name, &plan); err != nil {
			return out, err
		}
		h, err := strconv.ParseInt(plan.Height, 10, 64)
		if err != nil {
			return out, fmt.Errorf("parse height for %s: %w", name, err)
		}
		if h == 0 {
			continue // not applied / skipped
		}
		var blk blockResp
		if err := s.getJSON(ctx, "/cosmos/base/tendermint/v1beta1/blocks/"+strconv.FormatInt(h, 10), &blk); err != nil {
			return out, err
		}
		out = append(out, store.Upgrade{
			Name:            name,
			AppliedAtHeight: h,
			AppliedAtTime:   blk.Block.Header.Time,
			DecoderVersion:  versionToDecoder(name),
		})
	}
	return out, nil
}

// Sync calls Fetch then upserts each result. Returns the count upserted.
func (s *Syncer) Sync(ctx context.Context, st *store.Store, names []string) (int, error) {
	upgrades, err := s.Fetch(ctx, names)
	if err != nil {
		return 0, err
	}
	for _, u := range upgrades {
		if err := st.UpsertUpgrade(ctx, u); err != nil {
			return 0, err
		}
	}
	return len(upgrades), nil
}
