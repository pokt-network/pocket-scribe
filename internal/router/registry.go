package router

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0"
	v0_1_10 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_10"
	v0_1_20 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_20"
	v0_1_27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27"
	v0_1_28 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_28"
	v0_1_29 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_29"
	v0_1_30 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30"
	v0_1_34 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_34"
	v0_1_8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8"
)

// DefaultRegistry maps canonical decoder-version strings to their (stateless)
// Decoder. A new version's adapter is added here (add-decoder-version step 8).
func DefaultRegistry() map[string]decoders.Decoder {
	return map[string]decoders.Decoder{
		"v0_1_0":  v0_1_0.Decoder{},
		"v0_1_8":  v0_1_8.Decoder{},
		"v0_1_10": v0_1_10.Decoder{},
		"v0_1_20": v0_1_20.Decoder{},
		"v0_1_27": v0_1_27.Decoder{},
		"v0_1_28": v0_1_28.Decoder{},
		"v0_1_29": v0_1_29.Decoder{},
		"v0_1_30": v0_1_30.Decoder{},
		"v0_1_34": v0_1_34.Decoder{},
	}
}
