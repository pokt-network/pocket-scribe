package decoders

import (
	"github.com/cosmos/gogoproto/proto"

	sh27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/shared"
	sup27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
)

// init restores ONLY the enum name↔value maps that jsonpb needs
// (proto.EnumValueMap consults the global registry; stripregister removed the
// gen init() registrations). Registered from the NEWEST range tree (v0_1_27)
// because enum value sets only ever GROW (verified across v0_1_0..v0_1_33:
// RPCType +COMET_BFT@v0_1_27, SupplierUnbondingReason +MIGRATION@v0_1_13) — an
// older tree's map would reject newer names. enums_test.go enforces the
// superset property; add-decoder-version step: re-point these imports when a
// future version adds enum values. Init order is safe: Go guarantees the
// imported sh27/sup27 packages' var initialization runs before this init(),
// so the _name/_value maps are always populated; the nil guard only protects
// against a test binary that also loads an UNstripped tree.
func init() {
	reg := func(name string, nm map[int32]string, vm map[string]int32) {
		if proto.EnumValueMap(name) == nil {
			proto.RegisterEnum(name, nm, vm)
		}
	}
	reg("pocket.shared.RPCType", sh27.RPCType_name, sh27.RPCType_value)
	reg("pocket.shared.ConfigOptions", sh27.ConfigOptions_name, sh27.ConfigOptions_value)
	reg("pocket.supplier.SupplierUnbondingReason", sup27.SupplierUnbondingReason_name, sup27.SupplierUnbondingReason_value)
}
