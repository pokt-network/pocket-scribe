package proto_test

import (
	"testing"

	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
)

func TestBlockEnvelopeRoundtrip(t *testing.T) {
	in := &psv1.BlockEnvelope{
		Height: 135836, TimeUnixNano: 1748469041000000000,
		Hash: "dd01f0", ProposerAddress: "aa11bb", ChainId: "pocket",
		TxCount: 4, EventCount: 87, KvCount: 556, PublishedMsgCount: 647,
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out psv1.BlockEnvelope
	if err := out.Unmarshal(raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Height != in.Height || out.ChainId != in.ChainId || out.PublishedMsgCount != in.PublishedMsgCount {
		t.Fatalf("roundtrip mismatch: %+v vs %+v", &out, in)
	}
}

func TestEventInBlockBlockLevelSentinel(t *testing.T) {
	e := &psv1.EventInBlock{Event: []byte{0x0a}, TxIndex: -1, EventIndex: 3}
	raw, _ := e.Marshal()
	var out psv1.EventInBlock
	if err := out.Unmarshal(raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.TxIndex != -1 {
		t.Fatalf("tx_index sentinel lost: %d", out.TxIndex)
	}
}
