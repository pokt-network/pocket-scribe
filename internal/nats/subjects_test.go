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

func TestTxSubjectRoundtrip(t *testing.T) {
	s := TxSubject(135836, 3)
	if s != "pokt.tx.135836.3" {
		t.Fatalf("TxSubject = %q", s)
	}
	h, idx, err := HeightFromTxSubject(s)
	if err != nil || h != 135836 || idx != 3 {
		t.Fatalf("HeightFromTxSubject = %d,%d,%v", h, idx, err)
	}
}

func TestEventSubjectSanitizesType(t *testing.T) {
	s := EventSubject("pocket.supplier.EventSupplierStaked", 135836)
	if s != "pokt.events.pocket_supplier_EventSupplierStaked.135836" {
		t.Fatalf("EventSubject = %q", s)
	}
	h, err := HeightFromEventSubject(s)
	if err != nil || h != 135836 {
		t.Fatalf("HeightFromEventSubject = %d,%v", h, err)
	}
	if f := EventSubjectFilter("pocket.supplier.EventSupplierStaked"); f != "pokt.events.pocket_supplier_EventSupplierStaked.*" {
		t.Fatalf("EventSubjectFilter = %q", f)
	}
}

func TestKVSubjectRoundtrip(t *testing.T) {
	s := KVSubject("supplier", 135836)
	if s != "pokt.kv.supplier.135836" {
		t.Fatalf("KVSubject = %q", s)
	}
	h, err := HeightFromKVSubject(s)
	if err != nil || h != 135836 {
		t.Fatalf("HeightFromKVSubject = %d,%v", h, err)
	}
	if f := KVSubjectFilter("supplier"); f != "pokt.kv.supplier.*" {
		t.Fatalf("KVSubjectFilter = %q", f)
	}
}

// TestHeightFromTxSubjectErrors verifies all error branches in HeightFromTxSubject.
func TestHeightFromTxSubjectErrors(t *testing.T) {
	cases := []struct {
		subject string
		desc    string
	}{
		{"pokt.kv.supplier.1", "not a tx subject"},
		{"pokt.tx.1", "missing index token — only 1 part after prefix"},
		{"pokt.tx.abc.0", "non-numeric height"},
		{"pokt.tx.1.abc", "non-numeric tx index"},
	}
	for _, c := range cases {
		if _, _, err := HeightFromTxSubject(c.subject); err == nil {
			t.Errorf("HeightFromTxSubject(%q) [%s]: expected error", c.subject, c.desc)
		}
	}
}

// TestHeightFromEventSubjectErrors verifies all error branches.
func TestHeightFromEventSubjectErrors(t *testing.T) {
	cases := []struct {
		subject string
		desc    string
	}{
		{"pokt.kv.supplier.1", "not an event subject"},
		{"pokt.events.coin_spent", "only 1 part after prefix — no height"},
		{"pokt.events.coin_spent.notanumber", "non-numeric height"},
	}
	for _, c := range cases {
		if _, err := HeightFromEventSubject(c.subject); err == nil {
			t.Errorf("HeightFromEventSubject(%q) [%s]: expected error", c.subject, c.desc)
		}
	}
}

// TestHeightFromKVSubjectErrors verifies all error branches.
func TestHeightFromKVSubjectErrors(t *testing.T) {
	cases := []struct {
		subject string
		desc    string
	}{
		{"pokt.block.1", "not a kv subject"},
		{"pokt.kv.supplier", "only 1 part after prefix — no height"},
		{"pokt.kv.supplier.notanumber", "non-numeric height"},
	}
	for _, c := range cases {
		if _, err := HeightFromKVSubject(c.subject); err == nil {
			t.Errorf("HeightFromKVSubject(%q) [%s]: expected error", c.subject, c.desc)
		}
	}
}

// TestIsSubjectClassifiers verifies the Is*Subject helpers against canonical
// subjects from each grammar, plus a negative case for each (rule 7).
func TestIsSubjectClassifiers(t *testing.T) {
	blockS := BlockSubject(42)
	txS := TxSubject(42, 3)
	eventS := EventSubject("pocket.supplier.EventSupplierStaked", 42)
	kvS := KVSubject("supplier", 42)
	unknown := "pokt.unknown.42"

	cases := []struct {
		subject string
		block   bool
		tx      bool
		event   bool
		kv      bool
	}{
		{blockS, true, false, false, false},
		{txS, false, true, false, false},
		{eventS, false, false, true, false},
		{kvS, false, false, false, true},
		{unknown, false, false, false, false},
	}
	for _, c := range cases {
		if got := IsBlockSubject(c.subject); got != c.block {
			t.Errorf("IsBlockSubject(%q) = %v, want %v", c.subject, got, c.block)
		}
		if got := IsTxSubject(c.subject); got != c.tx {
			t.Errorf("IsTxSubject(%q) = %v, want %v", c.subject, got, c.tx)
		}
		if got := IsEventSubject(c.subject); got != c.event {
			t.Errorf("IsEventSubject(%q) = %v, want %v", c.subject, got, c.event)
		}
		if got := IsKVSubject(c.subject); got != c.kv {
			t.Errorf("IsKVSubject(%q) = %v, want %v", c.subject, got, c.kv)
		}
	}
}

func TestHeightFromSubjectDispatch(t *testing.T) {
	cases := []struct {
		subject string
		want    int64
		wantErr bool
	}{
		{"pokt.block.42", 42, false},
		{"pokt.tx.42.7", 42, false},
		{"pokt.events.pocket_supplier_EventSupplierStaked.42", 42, false},
		{"pokt.kv.supplier.42", 42, false},
		{"pokt.kv.supplier.notanumber", 0, true},
		{"pokt.unknown.42", 0, true},
		{"pokt.tx.42", 0, true}, // missing idx token
	}
	for _, c := range cases {
		h, err := HeightFromSubject(c.subject)
		if c.wantErr != (err != nil) || (!c.wantErr && h != c.want) {
			t.Errorf("HeightFromSubject(%q) = %d,%v want %d,err=%v", c.subject, h, err, c.want, c.wantErr)
		}
	}
}
