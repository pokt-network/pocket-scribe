package decoders

import (
	"encoding/binary"
	"testing"
)

func TestClassifySupplierKey(t *testing.T) {
	cases := []struct {
		key  string
		want SupplierKeyKind
	}{
		{"Supplier/operator_address/pokt16ar6g3w/", SupplierKeyRecord},
		{"Supplier/unbonding_height/pokt16ar6g3w/", SupplierKeyIgnore},
		{"ServiceConfigUpdate/service_id/eth/XXXXXXXX/pokt16ar/", SupplierKeySCURecord},
		{"ServiceConfigUpdate/operator_address/pokt16ar/eth/XXXXXXXX/", SupplierKeyIgnore},
		{"ServiceConfigUpdate/activation_height/XXXXXXXX/eth/pokt16ar/", SupplierKeyIgnore},
		{"ServiceConfigUpdate/deactivation_height/XXXXXXXX/eth/pokt16ar/XXXXXXXX/", SupplierKeyIgnore},
		{"p_supplier", SupplierKeyIgnore},
		{"garbage", SupplierKeyIgnore},
	}
	for _, c := range cases {
		if got := ClassifySupplierKey([]byte(c.key)); got != c.want {
			t.Errorf("ClassifySupplierKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}

func TestParseSCUPrimaryKey(t *testing.T) {
	var hbuf [8]byte
	binary.BigEndian.PutUint64(hbuf[:], 96801)
	key := append([]byte("ServiceConfigUpdate/service_id/arb_one/"), hbuf[:]...)
	key = append(key, []byte("/pokt12qse7etheight/")...)
	svc, act, op, err := ParseSCUPrimaryKey(key)
	if err != nil || svc != "arb_one" || act != 96801 || op != "pokt12qse7etheight" {
		t.Fatalf("ParseSCUPrimaryKey = %q,%d,%q,%v", svc, act, op, err)
	}
	if _, _, _, err := ParseSCUPrimaryKey([]byte("ServiceConfigUpdate/service_id/x")); err == nil {
		t.Fatal("want error on malformed key")
	}
}

// TestParseSCUPrimaryKeyNoHeightSegment verifies that a key with a service
// segment but no 8-byte height segment returns an error.
func TestParseSCUPrimaryKeyNoHeightSegment(t *testing.T) {
	// Only the service segment with trailing slash, no height bytes.
	key := []byte("ServiceConfigUpdate/service_id/eth/short")
	if _, _, _, err := ParseSCUPrimaryKey(key); err == nil {
		t.Fatal("want error: no height segment (fewer than 9 bytes after service/)")
	}
}

// TestParseSCUPrimaryKeyEmptyOperator verifies that a key with service and
// height but an empty operator segment returns an error.
func TestParseSCUPrimaryKeyEmptyOperator(t *testing.T) {
	var hbuf [8]byte
	binary.BigEndian.PutUint64(hbuf[:], 1000)
	// service="eth", 8-byte height, trailing slash, then nothing after the slash
	key := append([]byte("ServiceConfigUpdate/service_id/eth/"), hbuf[:]...)
	key = append(key, '/') // trailing slash with empty operator
	key = append(key, '/') // second slash to make j>0 branch produce empty string
	if _, _, _, err := ParseSCUPrimaryKey(key); err == nil {
		t.Fatal("want error: empty operator segment")
	}
}
