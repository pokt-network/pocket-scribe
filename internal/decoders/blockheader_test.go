package decoders

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
)

// frameDelimited marshals a gogo proto message with the SAME framing the
// FilePlugin / DecodeBlockHeader expects: a base-128 uvarint length prefix
// followed by the gogo Marshal() output. It uses the cometbft type's own Marshal
// (gogo), NOT google.golang.org/protobuf.
func frameDelimited(t *testing.T, req *abci.RequestFinalizeBlock) []byte {
	t.Helper()
	body, err := req.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(len(body)))
	out := make([]byte, 0, n+len(body))
	out = append(out, prefix[:n]...)
	out = append(out, body...)
	return out
}

// TestDecodeBlockHeaderRealSample is the golden test: it decodes real chain bytes
// (FilePlugin block-190974-meta) and asserts the exact projected values.
func TestDecodeBlockHeaderRealSample(t *testing.T) {
	raw, err := os.ReadFile("testdata/block-190974-meta")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	hdr, err := DecodeBlockHeader(raw)
	if err != nil {
		t.Fatalf("DecodeBlockHeader: %v", err)
	}
	if hdr.Height != 190974 {
		t.Fatalf("Height = %d, want 190974", hdr.Height)
	}
	if got := hdr.Time.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"); got != "2025-07-07T19:25:16.918434231Z" {
		t.Fatalf("Time = %q, want 2025-07-07T19:25:16.918434231Z", got)
	}
	const wantHash = "dd01f05916fc208c83bc273556f94f639852fee39967e2f99880c993b2740daa"
	if hdr.Hash != wantHash {
		t.Fatalf("Hash = %q, want %q", hdr.Hash, wantHash)
	}
	const wantProposer = "c067e15f0cb7d2ab48ebc0897d9b41e526700979"
	if hdr.ProposerAddress != wantProposer {
		t.Fatalf("ProposerAddress = %q, want %q", hdr.ProposerAddress, wantProposer)
	}
	if hdr.TxCount != 0 {
		t.Fatalf("TxCount = %d, want 0", hdr.TxCount)
	}
}

// TestDecodeBlockHeaderSyntheticRoundTrip constructs a header, frames it exactly
// as the FilePlugin would, decodes it, and asserts the projection round-trips.
// This guards the gogo-vs-google marshal trap and the hex projection.
func TestDecodeBlockHeaderSyntheticRoundTrip(t *testing.T) {
	// Hash is 32 bytes, ProposerAddress is 20 bytes (consensus sizes).
	want := &abci.RequestFinalizeBlock{
		Height:          42,
		Time:            mustUTC(t, "2026-06-09T12:34:56Z"),
		Hash:            []byte("0123456789abcdef0123456789abcdef"),
		ProposerAddress: []byte("proposeraddr00000000"),
		Txs:             [][]byte{[]byte("tx-a"), []byte("tx-b"), []byte("tx-c")},
	}
	hdr, err := DecodeBlockHeader(frameDelimited(t, want))
	if err != nil {
		t.Fatalf("DecodeBlockHeader: %v", err)
	}
	if hdr.Height != 42 {
		t.Fatalf("Height = %d, want 42", hdr.Height)
	}
	if !hdr.Time.Equal(want.Time) {
		t.Fatalf("Time = %s, want %s", hdr.Time, want.Time)
	}
	wantHash := hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	if hdr.Hash != wantHash {
		t.Fatalf("Hash = %q, want %q", hdr.Hash, wantHash)
	}
	if hdr.TxCount != 3 {
		t.Fatalf("TxCount = %d, want 3", hdr.TxCount)
	}
}

// TestReadDelimitedFraming documents the exact framing: a uvarint prefix then
// payload, with trailing bytes (subsequent records) left untouched.
func TestReadDelimitedFraming(t *testing.T) {
	raw, err := os.ReadFile("testdata/block-190974-meta")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	payload, consumed, err := readDelimited(raw)
	if err != nil {
		t.Fatalf("readDelimited: %v", err)
	}
	if len(payload) != 304 {
		t.Fatalf("payload len = %d, want 304", len(payload))
	}
	if consumed != 306 {
		t.Fatalf("consumed = %d, want 306 (2-byte uvarint + 304 payload)", consumed)
	}
	if consumed >= len(raw) {
		t.Fatal("meta file must have additional records after the header")
	}
}

func TestDecodeBlockHeaderEmptyMeta(t *testing.T) {
	if _, err := DecodeBlockHeader(nil); err == nil {
		t.Fatal("expected error for empty meta bytes")
	}
}

func TestDecodeBlockHeaderTruncated(t *testing.T) {
	// uvarint says 10 bytes follow, but only 2 are present.
	if _, err := DecodeBlockHeader([]byte{0x0a, 0x01, 0x02}); err == nil {
		t.Fatal("expected error for truncated record")
	}
}

func TestDecodeBlockHeaderOverflowPrefix(t *testing.T) {
	// 11 continuation bytes overflow a 64-bit uvarint.
	bad := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	if _, err := DecodeBlockHeader(bad); err == nil {
		t.Fatal("expected error for overflowing length prefix")
	}
}

func TestDecodeBlockHeaderRecordTooLarge(t *testing.T) {
	// A uvarint encoding a length above maxRecordSize must be rejected before alloc.
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(maxRecordSize)+1)
	if _, err := DecodeBlockHeader(prefix[:n]); err == nil {
		t.Fatal("expected error for record exceeding max size")
	}
}

func TestDecodeBlockHeaderBadProto(t *testing.T) {
	// A well-framed record whose payload is not a valid RequestFinalizeBlock.
	// 0x08 = field 1, varint wiretype, with no value -> Unmarshal errors.
	framed := append([]byte{0x01}, 0x08)
	if _, err := DecodeBlockHeader(framed); err == nil {
		t.Fatal("expected error for invalid proto payload")
	}
}

func TestDecodeBlockHeaderNonPositiveHeight(t *testing.T) {
	zero := &abci.RequestFinalizeBlock{Height: 0, Time: mustUTC(t, "2026-01-01T00:00:00Z")}
	if _, err := DecodeBlockHeader(frameDelimited(t, zero)); err == nil {
		t.Fatal("expected error for non-positive height")
	}
}

func mustUTC(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return ts.UTC()
}
