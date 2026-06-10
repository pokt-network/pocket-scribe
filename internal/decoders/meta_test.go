package decoders

import (
	"encoding/binary"
	"os"
	"testing"

	storetypes "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
)

// ---------------------------------------------------------------------------
// SplitMeta error paths
// ---------------------------------------------------------------------------

// TestSplitMetaExactlyThreeRecords verifies SplitMeta accepts a valid 3-record
// payload and returns the three slices without error.
func TestSplitMetaExactlyThreeRecords(t *testing.T) {
	meta := buildThreeRecordMeta(t)
	records, err := SplitMeta(meta)
	if err != nil {
		t.Fatalf("SplitMeta on valid 3-record payload: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("SplitMeta returned %d records, want 3", len(records))
	}
}

// TestSplitMetaWrongRecordCount verifies that a payload with only 2 records
// fails with the "want 3" record-count error.
func TestSplitMetaWrongRecordCount(t *testing.T) {
	meta := buildNRecordMeta(t, 2)
	_, err := SplitMeta(meta)
	if err == nil {
		t.Fatal("expected error: only 2 records in meta")
	}
}

// TestSplitMetaFourRecords verifies that 4 records also fails the count check.
func TestSplitMetaFourRecords(t *testing.T) {
	meta := buildNRecordMeta(t, 4)
	_, err := SplitMeta(meta)
	if err == nil {
		t.Fatal("expected error: 4 records in meta")
	}
}

// TestSplitMetaCorruptInnerRecord exercises the error path where one of the
// frames reports a length that exceeds the remaining bytes, causing
// readDelimited to fail and SplitMeta to wrap the error with a record index.
func TestSplitMetaCorruptInnerRecord(t *testing.T) {
	// Two valid records then a frame that claims 100 bytes but has none.
	meta := buildNRecordMeta(t, 2)
	badFrame := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(badFrame, 100) // claims 100 bytes
	meta = append(meta, badFrame[:n]...)  // no payload follows
	_, err := SplitMeta(meta)
	if err == nil {
		t.Fatal("expected error: truncated third record payload")
	}
}

// TestSplitMetaRealFixture verifies SplitMeta against the real chain fixture.
// Exercises the happy path including the zero-length third record (ResponseCommit).
func TestSplitMetaRealFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/block-190974-meta")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	records, err := SplitMeta(raw)
	if err != nil {
		t.Fatalf("SplitMeta on real fixture: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("SplitMeta returned %d records, want 3", len(records))
	}
	var req abci.RequestFinalizeBlock
	if err := req.Unmarshal(records[0]); err != nil {
		t.Fatalf("record[0] not RequestFinalizeBlock: %v", err)
	}
	if req.Height <= 0 {
		t.Fatalf("decoded height %d <= 0", req.Height)
	}
}

// ---------------------------------------------------------------------------
// ReadDelimited error paths
// ---------------------------------------------------------------------------

// TestReadDelimitedTruncatedPrefix exercises the "reading length prefix" error:
// an empty buffer should return a non-nil error immediately.
func TestReadDelimitedTruncatedPrefix(t *testing.T) {
	_, _, err := ReadDelimited([]byte{})
	if err == nil {
		t.Fatal("expected error for empty buffer")
	}
}

// TestReadDelimitedOverflowPrefix exercises the overflow path: 11 continuation
// bytes cause binary.Uvarint to return n<0.
func TestReadDelimitedOverflowPrefix(t *testing.T) {
	bad := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	_, _, err := ReadDelimited(bad)
	if err == nil {
		t.Fatal("expected error for overflowing uvarint prefix")
	}
}

// TestReadDelimitedExceedsMaxSize verifies that a length prefix exceeding
// maxRecordSize is rejected before any allocation.
func TestReadDelimitedExceedsMaxSize(t *testing.T) {
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(maxRecordSize)+1)
	_, _, err := ReadDelimited(prefix[:n])
	if err == nil {
		t.Fatal("expected error for record exceeding maxRecordSize")
	}
}

// TestReadDelimitedTruncatedPayload exercises the "need X bytes, have Y" path:
// the length prefix is valid but the payload is truncated.
func TestReadDelimitedTruncatedPayload(t *testing.T) {
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, 10) // claims 10 bytes
	buf := make([]byte, 0, n+2)
	buf = append(buf, prefix[:n]...)
	buf = append(buf, 0x01, 0x02)
	_, _, err := ReadDelimited(buf)
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}
}

// TestReadDelimitedHappyPath verifies the exported ReadDelimited wrapper
// returns consistent results with readDelimited on valid input.
func TestReadDelimitedHappyPath(t *testing.T) {
	payload := []byte{0x0a, 0x01, 0x02, 0x03}
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(len(payload)))
	buf := make([]byte, 0, n+len(payload)+2)
	buf = append(buf, prefix[:n]...)
	buf = append(buf, payload...)
	buf = append(buf, 0xde, 0xad) // trailing bytes (next record)

	got, consumed, err := ReadDelimited(buf)
	if err != nil {
		t.Fatalf("ReadDelimited: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %v want %v", got, payload)
	}
	if consumed != n+len(payload) {
		t.Fatalf("consumed = %d, want %d", consumed, n+len(payload))
	}
}

// ---------------------------------------------------------------------------
// StoreKeyOf error paths
// ---------------------------------------------------------------------------

// TestStoreKeyOfHappyPath verifies StoreKeyOf extracts the store_key field.
func TestStoreKeyOfHappyPath(t *testing.T) {
	kv := storetypes.StoreKVPair{
		StoreKey: "supplier",
		Key:      []byte("Supplier/operator_address/pokt1abc/"),
		Value:    []byte("value"),
		Delete:   false,
	}
	raw, err := kv.Marshal()
	if err != nil {
		t.Fatalf("marshal StoreKVPair: %v", err)
	}
	got, err := StoreKeyOf(raw)
	if err != nil {
		t.Fatalf("StoreKeyOf: %v", err)
	}
	if got != "supplier" {
		t.Fatalf("StoreKeyOf = %q, want \"supplier\"", got)
	}
}

// TestStoreKeyOfInvalidProto exercises the unmarshal error path: bytes that
// cannot be decoded as a StoreKVPair produce a non-nil error.
func TestStoreKeyOfInvalidProto(t *testing.T) {
	// 0xff is not a valid protobuf field tag — unmarshal must fail.
	_, err := StoreKeyOf([]byte{0xff, 0xfe, 0xfd, 0x00, 0x01, 0x02})
	if err == nil {
		t.Fatal("expected error for garbage StoreKVPair bytes")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildThreeRecordMeta assembles three uvarint-framed records: [req, resp, 0-byte commit].
func buildThreeRecordMeta(t *testing.T) []byte {
	t.Helper()
	return buildNRecordMeta(t, 3)
}

// buildNRecordMeta assembles n uvarint-framed records (each with a minimal payload).
// Index 2 is always the zero-length ResponseCommit (ADR-027).
func buildNRecordMeta(t *testing.T, n int) []byte {
	t.Helper()
	var out []byte
	for i := 0; i < n; i++ {
		if i == 2 {
			// third record is always 0-byte (ResponseCommit)
			out = append(out, 0x00)
			continue
		}
		var payload []byte
		if i == 0 {
			req := &abci.RequestFinalizeBlock{Height: int64(1 + i)}
			b, err := req.Marshal()
			if err != nil {
				t.Fatalf("marshal req: %v", err)
			}
			payload = b
		} else {
			resp := &abci.ResponseFinalizeBlock{}
			b, err := resp.Marshal()
			if err != nil {
				t.Fatalf("marshal resp: %v", err)
			}
			payload = b
		}
		prefix := make([]byte, binary.MaxVarintLen64)
		pn := binary.PutUvarint(prefix, uint64(len(payload)))
		out = append(out, prefix[:pn]...)
		out = append(out, payload...)
	}
	return out
}
