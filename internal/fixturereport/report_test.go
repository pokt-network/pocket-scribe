package fixturereport

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"

	"github.com/pokt-network/pocketscribe/internal/router"
)

func mustRouter(t *testing.T) router.Router {
	t.Helper()
	r, err := router.NewStaticRouter(MainnetUpgrades(), router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func fxPath(dir string, h int64, suffix string) string {
	return fmt.Sprintf("%s/block-%d-%s", dir, h, suffix)
}

func TestReportMatchesExistingExpected(t *testing.T) {
	r := mustRouter(t)
	// One quiet block and one busy supplier block from the existing corpus.
	for _, fx := range []struct {
		dir    string
		height int64
	}{
		{"../../test/fixtures/v0_1_20", 135297},
		{"../../test/fixtures/v0_1_28", 290584},
	} {
		meta, err := os.ReadFile(fxPath(fx.dir, fx.height, "meta"))
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(fxPath(fx.dir, fx.height, "data"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := Report(r, meta, data)
		if err != nil {
			t.Fatalf("Report(%d): %v", fx.height, err)
		}
		raw, err := os.ReadFile(fxPath(fx.dir, fx.height, "expected.json"))
		if err != nil {
			t.Fatal(err)
		}
		var want Result
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("Report(%d) mismatch:\ngot  %+v\nwant %+v", fx.height, *got, want)
		}
	}
}

func TestReportErrorPaths(t *testing.T) {
	r := mustRouter(t)
	if _, err := Report(r, []byte("garbage"), nil); err == nil {
		t.Fatal("corrupt meta must error")
	}
	if _, err := Report(r, nil, nil); err == nil {
		t.Fatal("empty meta must error")
	}
}

// buildMeta builds a valid 3-record meta payload from a req + resp.
func buildMeta(t *testing.T, req *abci.RequestFinalizeBlock, resp *abci.ResponseFinalizeBlock) []byte {
	t.Helper()
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
		result := make([]byte, n+len(b))
		copy(result, prefix[:n])
		copy(result[n:], b)
		return result
	}
	meta := make([]byte, 0, len(reqBytes)+len(respBytes)+binary.MaxVarintLen64*2+1)
	meta = append(meta, frame(reqBytes)...)
	meta = append(meta, frame(respBytes)...)
	meta = append(meta, 0x00) // zero-length ResponseCommit
	return meta
}

// frameKV produces a uvarint-framed KV payload from raw bytes.
func frameKV(data []byte) []byte {
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(len(data)))
	result := make([]byte, n+len(data))
	copy(result, prefix[:n])
	copy(result[n:], data)
	return result
}

// TestReportSyntheticBranches exercises the per-item skip paths that real
// fixtures cannot hit (failed tx code, bad KV framing, tx Body==nil).
func TestReportSyntheticBranches(t *testing.T) {
	r := mustRouter(t)

	// Build a block with: one failed tx (code!=0), one tx with garbage bytes
	// (Unmarshal fails → tx.Body check never reached via that path, but the
	// Unmarshal-succeeds/nil-body path is exercised by an empty-slice tx below).
	nilBodyTx, err := (&abci.RequestFinalizeBlock{Height: 99}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	req := &abci.RequestFinalizeBlock{
		Height: 1,
		Time:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Hash:   make([]byte, 32),
		// tx[0]: failed (code=1 → skip), tx[1]: success but garbage bytes
		// (Unmarshal fails → continue), tx[2]: success but not a sdktx.Tx
		// with body → tx.Body==nil after Unmarshal produces empty struct
		Txs: [][]byte{{0xff, 0xfe}, {0xff, 0xfe}, nilBodyTx},
	}
	resp := &abci.ResponseFinalizeBlock{
		TxResults: []*abci.ExecTxResult{
			{Code: 1}, // failed → skipped
			{Code: 0}, // success but garbage bytes → Unmarshal fails → continue
			{Code: 0}, // success, valid proto but not a Tx → Body==nil → continue
		},
	}
	meta := buildMeta(t, req, resp)

	// data with a valid uvarint-framed payload (exercises the non-supplier-store
	// path) followed by a truncated record (exercises the ReadDelimited error break).
	// First record: valid framing, non-supplier store key.
	nonSupplierKV := frameKV([]byte{}) // zero-byte payload → valid uvarint decode, unmarshal → empty
	// Second record: corrupt framing (overflowing uvarint) → ReadDelimited error → break.
	corruptFrame := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
	corruptData := make([]byte, 0, len(nonSupplierKV)+len(corruptFrame))
	corruptData = append(corruptData, nonSupplierKV...)
	corruptData = append(corruptData, corruptFrame...)

	got, err := Report(r, meta, corruptData)
	if err != nil {
		t.Fatalf("Report with skip-paths: %v", err)
	}
	if got.Height != 1 {
		t.Fatalf("height = %d, want 1", got.Height)
	}
	if got.TxCount != 3 {
		t.Fatalf("tx_count = %d, want 3", got.TxCount)
	}
	if got.Supplier != nil {
		t.Fatalf("supplier must be nil for no-activity block")
	}
}

// TestReportTxCountMoreThanResults exercises the txIdx >= len(resp.TxResults)
// guard in the tx decode loop.
func TestReportTxCountMoreThanResults(t *testing.T) {
	r := mustRouter(t)
	req := &abci.RequestFinalizeBlock{
		Height: 1,
		Time:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Hash:   make([]byte, 32),
		Txs:    [][]byte{{0x01}, {0x02}}, // 2 txs
	}
	resp := &abci.ResponseFinalizeBlock{
		TxResults: []*abci.ExecTxResult{{Code: 0}}, // only 1 result
	}
	meta := buildMeta(t, req, resp)
	got, err := Report(r, meta, nil)
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if got.TxCount != 2 {
		t.Fatalf("tx_count = %d, want 2", got.TxCount)
	}
}

// buildMetaRaw constructs a 3-record meta using raw bytes for records[0..2].
func buildMetaRaw(rec0, rec1, rec2 []byte) []byte {
	frame := func(b []byte) []byte {
		prefix := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(prefix, uint64(len(b)))
		result := make([]byte, n+len(b))
		copy(result, prefix[:n])
		copy(result[n:], b)
		return result
	}
	meta := make([]byte, 0, len(rec0)+len(rec1)+len(rec2)+binary.MaxVarintLen64*3)
	meta = append(meta, frame(rec0)...)
	meta = append(meta, frame(rec1)...)
	meta = append(meta, frame(rec2)...)
	return meta
}

// TestReportSplitMetaError exercises the SplitMeta error path: a meta whose
// first record is a valid RequestFinalizeBlock but only has 2 records total
// (ADR-027 requires exactly 3).
func TestReportSplitMetaError(t *testing.T) {
	r := mustRouter(t)
	req := &abci.RequestFinalizeBlock{
		Height: 1,
		Time:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Hash:   make([]byte, 32),
	}
	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	// Only 2 records instead of 3 — SplitMeta will fail.
	frame := func(b []byte) []byte {
		prefix := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(prefix, uint64(len(b)))
		return append(prefix[:n:n], b...)
	}
	twoRecordMeta := append(frame(reqBytes), frame(reqBytes)...)
	if _, err := Report(r, twoRecordMeta, nil); err == nil {
		t.Fatal("2-record meta must produce a SplitMeta error")
	}
}

// TestReportRespUnmarshalError exercises the resp.Unmarshal error path: a meta
// with a valid RequestFinalizeBlock in record[0] and [2], but garbage bytes in
// record[1] which should unmarshal as ResponseFinalizeBlock.
func TestReportRespUnmarshalError(t *testing.T) {
	r := mustRouter(t)
	req := &abci.RequestFinalizeBlock{
		Height: 1,
		Time:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Hash:   make([]byte, 32),
	}
	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	// Record[1] is garbage that will cause ResponseFinalizeBlock.Unmarshal to fail.
	// Use bytes that form a valid uvarint frame but invalid protobuf for resp.
	// ResponseFinalizeBlock field 1 is repeated bytes — a mismatched wire type
	// triggers an error.
	badRespBytes := []byte{0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08} // field 1, wire type 1 (64-bit) — wrong for repeated bytes
	meta := buildMetaRaw(reqBytes, badRespBytes, []byte{})
	if _, err := Report(r, meta, nil); err == nil {
		t.Fatal("bad resp record must produce an unmarshal error")
	}
}

// TestReportKVUnmarshalError exercises the kv.Unmarshal error path: a valid
// uvarint-framed payload whose content is not a valid StoreKVPair.
func TestReportKVUnmarshalError(t *testing.T) {
	r := mustRouter(t)
	req := &abci.RequestFinalizeBlock{
		Height: 1,
		Time:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Hash:   make([]byte, 32),
	}
	resp := &abci.ResponseFinalizeBlock{}
	meta := buildMeta(t, req, resp)

	// A uvarint-framed record whose content cannot unmarshal as StoreKVPair.
	// Field 1 of StoreKVPair is store_key (string, wire type 2).
	// Inject wire type 1 (varint) for field 1 to trigger an unmarshal error.
	badKV := frameKV([]byte{0x08, 0x01}) // field 1, wire type 0 (varint) — wrong for string
	got, err := Report(r, meta, badKV)
	if err != nil {
		t.Fatalf("Report must not error on bad KV (skip-continue semantics): %v", err)
	}
	if got.Supplier != nil {
		t.Fatal("bad KV must not produce supplier activity")
	}
}

func TestMainnetUpgradesComplete(t *testing.T) {
	ups := MainnetUpgrades()
	if len(ups) != 31 {
		t.Fatalf("MainnetUpgrades has %d entries, want 31 (v0.1.2..v0.1.31, v0.1.33; v0.1.1 and v0.1.32 never applied)", len(ups))
	}
	// Heights strictly increasing (sanity vs versions.yaml transcription).
	for i := 1; i < len(ups); i++ {
		if ups[i].AppliedAtHeight <= ups[i-1].AppliedAtHeight {
			t.Fatalf("non-monotonic heights at %s", ups[i].Name)
		}
	}
}
