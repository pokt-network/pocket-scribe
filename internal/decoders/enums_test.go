package decoders

import (
	"testing"

	"github.com/cosmos/gogoproto/proto"

	sh0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/shared"
	sup0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/supplier"
	sh8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	sup8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
)

// TestRegisteredEnumsAreSupersets: the centrally-registered (newest-range) enum
// maps must contain every name of every older range's maps, or jsonpb decode of
// older-era events would fail on a valid name.
func TestRegisteredEnumsAreSupersets(t *testing.T) {
	cases := []struct {
		name string
		olds []map[string]int32
	}{
		{"pocket.shared.RPCType", []map[string]int32{sh0.RPCType_value, sh8.RPCType_value}},
		{"pocket.shared.ConfigOptions", []map[string]int32{sh0.ConfigOptions_value, sh8.ConfigOptions_value}},
		{"pocket.supplier.SupplierUnbondingReason", []map[string]int32{sup0.SupplierUnbondingReason_value, sup8.SupplierUnbondingReason_value}},
	}
	for _, c := range cases {
		reg := proto.EnumValueMap(c.name)
		if reg == nil {
			t.Fatalf("enum %s not registered (enums.go init missing?)", c.name)
		}
		for i, old := range c.olds {
			for name, val := range old {
				got, ok := reg[name]
				if !ok || got != val {
					t.Errorf("enum %s: older-range[%d] name %q=%d not in registered map (got %d, ok=%v)", c.name, i, name, val, got, ok)
				}
			}
		}
	}
}
