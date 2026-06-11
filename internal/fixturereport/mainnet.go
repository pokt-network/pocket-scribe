package fixturereport

import "github.com/pokt-network/pocketscribe/internal/router"

// MainnetUpgrades is the chain-authoritative mainnet upgrade table (source:
// pocketscribe-mainnet-archeology bucket versions.yaml, verified against the
// Sauron LCD applied_plan endpoints 2026-05-22). DecoderVersion is the
// uniform decoder-dir spelling of the tag; the router's lenient fallback maps
// unregistered versions to the nearest earlier registered decoder, which the
// break map (docs/research/supplier-shape-breaks.md) proves shape-safe.
// v0.1.1 and v0.1.32 were never applied on mainnet and are deliberately absent.
func MainnetUpgrades() []router.Upgrade {
	return []router.Upgrade{
		{Name: "v0.1.2", AppliedAtHeight: 78621, DecoderVersion: "v0_1_2"},
		{Name: "v0.1.3", AppliedAtHeight: 78632, DecoderVersion: "v0_1_3"},
		{Name: "v0.1.4", AppliedAtHeight: 78641, DecoderVersion: "v0_1_4"},
		{Name: "v0.1.5", AppliedAtHeight: 78654, DecoderVersion: "v0_1_5"},
		{Name: "v0.1.6", AppliedAtHeight: 78659, DecoderVersion: "v0_1_6"},
		{Name: "v0.1.7", AppliedAtHeight: 78665, DecoderVersion: "v0_1_7"},
		{Name: "v0.1.8", AppliedAtHeight: 78671, DecoderVersion: "v0_1_8"},
		{Name: "v0.1.9", AppliedAtHeight: 78678, DecoderVersion: "v0_1_9"},
		{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
		{Name: "v0.1.11", AppliedAtHeight: 78689, DecoderVersion: "v0_1_11"},
		{Name: "v0.1.12", AppliedAtHeight: 78697, DecoderVersion: "v0_1_12"},
		{Name: "v0.1.13", AppliedAtHeight: 80510, DecoderVersion: "v0_1_13"},
		{Name: "v0.1.14", AppliedAtHeight: 93825, DecoderVersion: "v0_1_14"},
		{Name: "v0.1.15", AppliedAtHeight: 94370, DecoderVersion: "v0_1_15"},
		{Name: "v0.1.16", AppliedAtHeight: 99293, DecoderVersion: "v0_1_16"},
		{Name: "v0.1.17", AppliedAtHeight: 102142, DecoderVersion: "v0_1_17"},
		{Name: "v0.1.18", AppliedAtHeight: 116100, DecoderVersion: "v0_1_18"},
		{Name: "v0.1.19", AppliedAtHeight: 117454, DecoderVersion: "v0_1_19"},
		{Name: "v0.1.20", AppliedAtHeight: 135297, DecoderVersion: "v0_1_20"},
		{Name: "v0.1.21", AppliedAtHeight: 138931, DecoderVersion: "v0_1_21"},
		{Name: "v0.1.22", AppliedAtHeight: 155173, DecoderVersion: "v0_1_22"},
		{Name: "v0.1.23", AppliedAtHeight: 161109, DecoderVersion: "v0_1_23"},
		{Name: "v0.1.24", AppliedAtHeight: 161169, DecoderVersion: "v0_1_24"},
		{Name: "v0.1.25", AppliedAtHeight: 190974, DecoderVersion: "v0_1_25"},
		{Name: "v0.1.26", AppliedAtHeight: 190979, DecoderVersion: "v0_1_26"},
		{Name: "v0.1.27", AppliedAtHeight: 247893, DecoderVersion: "v0_1_27"},
		{Name: "v0.1.28", AppliedAtHeight: 287932, DecoderVersion: "v0_1_28"},
		{Name: "v0.1.29", AppliedAtHeight: 382250, DecoderVersion: "v0_1_29"},
		{Name: "v0.1.30", AppliedAtHeight: 484473, DecoderVersion: "v0_1_30"},
		{Name: "v0.1.31", AppliedAtHeight: 635506, DecoderVersion: "v0_1_31"},
		{Name: "v0.1.33", AppliedAtHeight: 703870, DecoderVersion: "v0_1_33"},
		{Name: "v0.1.34", AppliedAtHeight: 788945, DecoderVersion: "v0_1_34"},
	}
}
