package fileplugin

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	storetypes "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
)

// ---------------------------------------------------------------------------
// parseMetaHeight
// ---------------------------------------------------------------------------

// TestParseMetaHeightHappy verifies block-{H}-meta filenames are parsed correctly.
func TestParseMetaHeightHappy(t *testing.T) {
	cases := []struct {
		name string
		want int64
	}{
		{"block-1-meta", 1},
		{"block-135836-meta", 135836},
		{"block-9999999-meta", 9999999},
	}
	for _, c := range cases {
		h, err := parseMetaHeight(c.name)
		if err != nil {
			t.Errorf("parseMetaHeight(%q): %v", c.name, err)
			continue
		}
		if h != c.want {
			t.Errorf("parseMetaHeight(%q) = %d, want %d", c.name, h, c.want)
		}
	}
}

// TestParseMetaHeightErrors verifies non-conforming filenames return errors.
func TestParseMetaHeightErrors(t *testing.T) {
	bad := []string{
		"block-abc-meta", // non-numeric height
		"block--meta",    // empty height
		"block-1-data",   // wrong suffix
		"noblock-1-meta", // wrong prefix
		"just-a-file",    // no recognisable parts
	}
	for _, name := range bad {
		if _, err := parseMetaHeight(name); err == nil {
			t.Errorf("parseMetaHeight(%q) should have returned error", name)
		}
	}
}

// ---------------------------------------------------------------------------
// fanOutHeight — error injection tests
// ---------------------------------------------------------------------------

// TestFanOutHeightMissingDataFile verifies that a missing -data file returns an
// error (FilePlugin always writes both; a missing data file is abnormal).
func TestFanOutHeightMissingDataFile(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-1-meta")

	// Write only the meta file (missing the -data companion).
	if err := os.WriteFile(metaPath, buildMinimalMeta(t), 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	publish := func(_ string, _ []byte, _ string, _ int64) error { return nil }
	_, err := fanOutHeight(nil, publish, 1, metaPath, "testchain") //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error for missing data file")
	}
	if !strings.Contains(err.Error(), "read data") {
		t.Fatalf("error should mention 'read data': %v", err)
	}
}

// TestFanOutHeightCorruptMeta verifies that a corrupt meta file (invalid
// uvarint framing) returns an error from SplitMeta.
func TestFanOutHeightCorruptMeta(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-1-meta")
	// Write garbage that can't be parsed as uvarint-delimited records.
	if err := os.WriteFile(metaPath, []byte{0xff, 0xfe, 0x00}, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"
	if err := os.WriteFile(dataPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	publish := func(_ string, _ []byte, _ string, _ int64) error { return nil }
	_, err := fanOutHeight(nil, publish, 1, metaPath, "testchain") //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error for corrupt meta file")
	}
}

// TestFanOutHeightPublishError verifies that a publish failure is propagated
// when the injected publish function returns an error.
func TestFanOutHeightPublishError(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-1-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	meta := buildMinimalMeta(t)
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	// Write a valid (empty) data file.
	if err := os.WriteFile(dataPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	publishErr := errors.New("nats: server unavailable")
	publishCalls := 0
	publish := func(_ string, _ []byte, _ string, _ int64) error {
		publishCalls++
		return publishErr
	}
	_, err := fanOutHeight(nil, publish, 1, metaPath, "testchain") //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error when publish fails")
	}
	if publishCalls == 0 {
		t.Fatal("expected publish to be called at least once")
	}
}

// TestFanOutHeightHappyNoTxs verifies that a height with no txs, no events, and
// no KV pairs publishes exactly 1 message (the envelope).
func TestFanOutHeightHappyNoTxs(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-42-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	meta := buildMinimalMeta(t)
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err := os.WriteFile(dataPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	var published []string
	publish := func(subj string, _ []byte, _ string, _ int64) error {
		published = append(published, subj)
		return nil
	}
	n, err := fanOutHeight(nil, publish, 42, metaPath, "testchain") //nolint:staticcheck
	if err != nil {
		t.Fatalf("fanOutHeight: %v", err)
	}
	// Only the block envelope should be published.
	if n != 1 {
		t.Fatalf("n = %d, want 1 (only the envelope)", n)
	}
	if len(published) != 1 {
		t.Fatalf("published = %v, want exactly 1 message", published)
	}
	if published[0] != "pokt.block.42" {
		t.Fatalf("envelope subject = %q, want pokt.block.42", published[0])
	}
}

// TestFanOutHeightWithKVPair verifies that a height with one KV pair in the
// data file publishes 2 messages: the KV + the envelope.
func TestFanOutHeightWithKVPair(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-43-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	meta := buildMinimalMeta(t)
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	data := buildKVData(t, "supplier", []byte("Supplier/operator_address/pokt1abc/"), []byte("value"))
	if err := os.WriteFile(dataPath, data, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	var published []string
	publish := func(subj string, _ []byte, _ string, _ int64) error {
		published = append(published, subj)
		return nil
	}
	n, err := fanOutHeight(nil, publish, 43, metaPath, "testchain") //nolint:staticcheck
	if err != nil {
		t.Fatalf("fanOutHeight: %v", err)
	}
	// KV message + envelope = 2
	if n != 2 {
		t.Fatalf("n = %d, want 2 (1 KV + envelope)", n)
	}
	hasKV, hasBlock := false, false
	for _, s := range published {
		if strings.HasPrefix(s, "pokt.kv.supplier.") {
			hasKV = true
		}
		if s == "pokt.block.43" {
			hasBlock = true
		}
	}
	if !hasKV {
		t.Errorf("no KV subject published, got %v", published)
	}
	if !hasBlock {
		t.Errorf("no block envelope published, got %v", published)
	}
}

// TestFanOutHeightWithTxAndEvents verifies that a height with 1 tx and 1
// block-level event publishes: 1 tx msg + 1 event msg + 1 envelope = 3 total.
func TestFanOutHeightWithTxAndEvents(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-44-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	txBytes := []byte("fake-tx-bytes")
	txResultBytes, err := (&abci.ExecTxResult{Code: 0}).Marshal()
	if err != nil {
		t.Fatalf("marshal tx result: %v", err)
	}
	blockEvent := abci.Event{Type: "coin_spent", Attributes: []abci.EventAttribute{{Key: "amount", Value: "100upokt"}}}
	req := &abci.RequestFinalizeBlock{
		Height: 44,
		Hash:   make([]byte, 32),
		Time:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Txs:    [][]byte{txBytes},
	}
	resp := &abci.ResponseFinalizeBlock{
		TxResults: []*abci.ExecTxResult{{Code: 0, Data: txResultBytes}},
		Events:    []abci.Event{blockEvent},
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	respBytes, err := resp.Marshal()
	if err != nil {
		t.Fatalf("marshal resp: %v", err)
	}
	meta := buildThreeRecordMetaWithPayloads(t, reqBytes, respBytes)
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err := os.WriteFile(dataPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	var published []string
	publish := func(subj string, _ []byte, _ string, _ int64) error {
		published = append(published, subj)
		return nil
	}
	n, err := fanOutHeight(nil, publish, 44, metaPath, "testchain") //nolint:staticcheck
	if err != nil {
		t.Fatalf("fanOutHeight: %v", err)
	}
	// 1 tx + 1 block-event + 1 envelope = 3
	if n != 3 {
		t.Fatalf("n = %d, want 3 (1 tx + 1 block-event + envelope)", n)
	}
	hasTx, hasEvent, hasBlock := false, false, false
	for _, s := range published {
		if strings.HasPrefix(s, "pokt.tx.44.") {
			hasTx = true
		}
		if strings.HasPrefix(s, "pokt.events.") {
			hasEvent = true
		}
		if s == "pokt.block.44" {
			hasBlock = true
		}
	}
	if !hasTx {
		t.Errorf("no tx subject published, got %v", published)
	}
	if !hasEvent {
		t.Errorf("no event subject published, got %v", published)
	}
	if !hasBlock {
		t.Errorf("no block envelope published, got %v", published)
	}
}

// TestFanOutHeightPublishErrorOnEvent verifies that a publish failure on an event
// message propagates correctly.
func TestFanOutHeightPublishErrorOnEvent(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-45-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	blockEvent := abci.Event{Type: "coin_spent"}
	req := &abci.RequestFinalizeBlock{Height: 45, Hash: make([]byte, 32), Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	resp := &abci.ResponseFinalizeBlock{Events: []abci.Event{blockEvent}}

	reqBytes, _ := req.Marshal()
	respBytes, _ := resp.Marshal()
	meta := buildThreeRecordMetaWithPayloads(t, reqBytes, respBytes)
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err := os.WriteFile(dataPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	callCount := 0
	publish := func(subj string, _ []byte, _ string, _ int64) error {
		callCount++
		if strings.HasPrefix(subj, "pokt.events.") {
			return errors.New("publish event failed")
		}
		return nil
	}
	_, err := fanOutHeight(nil, publish, 45, metaPath, "testchain") //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error when event publish fails")
	}
}

// TestFanOutHeightCorruptRequestProto verifies that a valid meta framing but a
// corrupt RequestFinalizeBlock proto payload (unparseable bytes) returns an error.
func TestFanOutHeightCorruptRequestProto(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-1-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	// Build meta with 3 records but record[0] is corrupt proto bytes.
	badMeta := buildThreeRecordMetaWithPayloads(t,
		[]byte{0xff, 0xfe, 0x00}, // garbage record[0] — not a valid RequestFinalizeBlock
		mustMarshalResp(t, &abci.ResponseFinalizeBlock{}),
	)
	if err := os.WriteFile(metaPath, badMeta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err := os.WriteFile(dataPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	publish := func(_ string, _ []byte, _ string, _ int64) error { return nil }
	_, err := fanOutHeight(nil, publish, 1, metaPath, "testchain") //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error for corrupt RequestFinalizeBlock proto")
	}
	if !strings.Contains(err.Error(), "RequestFinalizeBlock") {
		t.Fatalf("error should mention RequestFinalizeBlock: %v", err)
	}
}

// TestFanOutHeightCorruptResponseProto verifies that a corrupt ResponseFinalizeBlock
// in record[1] returns an error.
func TestFanOutHeightCorruptResponseProto(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-1-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	req := &abci.RequestFinalizeBlock{Height: 1, Hash: make([]byte, 32)}
	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	badMeta := buildThreeRecordMetaWithPayloads(t,
		reqBytes,
		[]byte{0xff, 0xfe, 0x00}, // garbage record[1]
	)
	if err := os.WriteFile(metaPath, badMeta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err := os.WriteFile(dataPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	publish := func(_ string, _ []byte, _ string, _ int64) error { return nil }
	_, err = fanOutHeight(nil, publish, 1, metaPath, "testchain") //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error for corrupt ResponseFinalizeBlock proto")
	}
	if !strings.Contains(err.Error(), "ResponseFinalizeBlock") {
		t.Fatalf("error should mention ResponseFinalizeBlock: %v", err)
	}
}

// TestFanOutHeightCorruptKVRecord verifies that a data file with an invalid
// uvarint-framed record returns an error when the record is being processed.
func TestFanOutHeightCorruptKVRecord(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-1-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	meta := buildMinimalMeta(t)
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	// Write a frame that claims 100 bytes but has 0.
	badData := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(badData, 100)
	badData = badData[:n] // no payload
	if err := os.WriteFile(dataPath, badData, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	publish := func(_ string, _ []byte, _ string, _ int64) error { return nil }
	_, err := fanOutHeight(nil, publish, 1, metaPath, "testchain") //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error for truncated KV record")
	}
}

// TestFanOutHeightBlockTimeStamped verifies that blockTimeNano is non-zero and
// equals header.Time.UnixNano() for every message published (ADR-022 amendment).
func TestFanOutHeightBlockTimeStamped(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "block-100-meta")
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"

	blockTime := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	wantNano := blockTime.UnixNano()

	txBytes := []byte("fake-tx")
	blockEvent := abci.Event{Type: "transfer"}
	req := &abci.RequestFinalizeBlock{
		Height: 100,
		Hash:   make([]byte, 32),
		Time:   blockTime,
		Txs:    [][]byte{txBytes},
	}
	resp := &abci.ResponseFinalizeBlock{
		TxResults: []*abci.ExecTxResult{{Code: 0}},
		Events:    []abci.Event{blockEvent},
	}
	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	respBytes, err := resp.Marshal()
	if err != nil {
		t.Fatalf("marshal resp: %v", err)
	}
	meta := buildThreeRecordMetaWithPayloads(t, reqBytes, respBytes)
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	// One KV entry so we test all four fan-out paths.
	data := buildKVData(t, "application", []byte("key"), []byte("val"))
	if err := os.WriteFile(dataPath, data, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	var gotNanos []int64
	publish := func(_ string, _ []byte, _ string, blockTimeNano int64) error {
		gotNanos = append(gotNanos, blockTimeNano)
		return nil
	}
	n, err := fanOutHeight(nil, publish, 100, metaPath, "testchain") //nolint:staticcheck
	if err != nil {
		t.Fatalf("fanOutHeight: %v", err)
	}
	// 1 tx + 1 event + 1 kv + 1 envelope = 4
	if n != 4 {
		t.Fatalf("n = %d, want 4", n)
	}
	if len(gotNanos) != 4 {
		t.Fatalf("publish called %d times, want 4", len(gotNanos))
	}
	for i, nano := range gotNanos {
		if nano <= 0 {
			t.Errorf("message[%d] blockTimeNano = %d, want > 0", i, nano)
		}
		if nano != wantNano {
			t.Errorf("message[%d] blockTimeNano = %d, want %d", i, nano, wantNano)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildThreeRecordMetaWithPayloads builds a 3-record meta payload using
// explicitly provided bytes for record[0] and record[1]. record[2] is always 0-byte.
func buildThreeRecordMetaWithPayloads(t *testing.T, rec0, rec1 []byte) []byte {
	t.Helper()
	frame := func(b []byte) []byte {
		prefix := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(prefix, uint64(len(b)))
		return append(prefix[:n], b...)
	}
	meta := make([]byte, 0, len(rec0)+len(rec1)+binary.MaxVarintLen64*2+1)
	meta = append(meta, frame(rec0)...)
	meta = append(meta, frame(rec1)...)
	meta = append(meta, 0x00)
	return meta
}

// mustMarshalResp marshals an abci.ResponseFinalizeBlock or fatals.
func mustMarshalResp(t *testing.T, resp *abci.ResponseFinalizeBlock) []byte {
	t.Helper()
	b, err := resp.Marshal()
	if err != nil {
		t.Fatalf("marshal resp: %v", err)
	}
	return b
}

// buildMinimalMeta constructs a valid 3-record meta payload:
// [RequestFinalizeBlock{Height:1}, ResponseFinalizeBlock{}, 0-byte ResponseCommit]
func buildMinimalMeta(t *testing.T) []byte {
	t.Helper()
	req := &abci.RequestFinalizeBlock{
		Height: 1,
		Time:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Hash:   make([]byte, 32),
	}
	resp := &abci.ResponseFinalizeBlock{}

	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	respBytes, err := resp.Marshal()
	if err != nil {
		t.Fatalf("marshal resp: %v", err)
	}

	frame := func(b []byte) []byte {
		prefix := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(prefix, uint64(len(b)))
		out := make([]byte, 0, n+len(b))
		out = append(out, prefix[:n]...)
		out = append(out, b...)
		return out
	}

	meta := make([]byte, 0, len(reqBytes)+len(respBytes)+binary.MaxVarintLen64*2+1)
	meta = append(meta, frame(reqBytes)...)
	meta = append(meta, frame(respBytes)...)
	meta = append(meta, 0x00) // zero-length ResponseCommit
	return meta
}

// buildKVData constructs a uvarint-framed data payload containing one StoreKVPair.
func buildKVData(t *testing.T, storeKey string, key, value []byte) []byte {
	t.Helper()
	kv := storetypes.StoreKVPair{
		StoreKey: storeKey,
		Key:      key,
		Value:    value,
		Delete:   false,
	}
	raw, err := kv.Marshal()
	if err != nil {
		t.Fatalf("marshal StoreKVPair: %v", err)
	}
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(len(raw)))
	out := make([]byte, 0, n+len(raw))
	out = append(out, prefix[:n]...)
	out = append(out, raw...)
	return out
}
