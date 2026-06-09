package nats

import "testing"

func TestBlockSubjectRoundTrip(t *testing.T) {
	subj := BlockSubject(635505)
	if subj != "pokt.block.635505" {
		t.Fatalf("BlockSubject = %q, want pokt.block.635505", subj)
	}
	h, err := HeightFromBlockSubject(subj)
	if err != nil {
		t.Fatalf("HeightFromBlockSubject: %v", err)
	}
	if h != 635505 {
		t.Fatalf("height = %d, want 635505", h)
	}
}

func TestHeightFromBadSubject(t *testing.T) {
	for _, bad := range []string{"pokt.block.", "pokt.block.abc", "pokt.tx.5.0", "garbage"} {
		if _, err := HeightFromBlockSubject(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

func TestMsgIDDeterministic(t *testing.T) {
	a := MsgID(BlockSubject(5), 5, 0)
	b := MsgID(BlockSubject(5), 5, 0)
	if a != b {
		t.Fatalf("MsgID not deterministic: %q != %q", a, b)
	}
	if a == MsgID(BlockSubject(6), 6, 0) {
		t.Fatal("MsgID collided across heights")
	}
}
