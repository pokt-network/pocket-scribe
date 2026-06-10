package decoders

import (
	"strings"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// sdkBookkeepingAttrs are SDK-injected attributes that are NOT proto fields of
// typed events (spike finding §4c): "mode" on block-level events
// (BeginBlock/EndBlock) and "msg_index" on tx events.
func isBookkeepingAttr(key string) bool { return key == "mode" || key == "msg_index" }

// EventAttrsJSON rebuilds the typed-event JSON document from its ABCI
// attributes: {"<field>":<raw json>,...}. Attribute values of typed events ARE
// raw JSON (quoted int64s, enum names, embedded objects) — they are spliced in
// verbatim, never re-encoded (fidelity).
func EventAttrsJSON(attrs []types.EventAttr) []byte {
	parts := make([]string, 0, len(attrs))
	for _, a := range attrs {
		if isBookkeepingAttr(a.Key) {
			continue
		}
		parts = append(parts, `"`+a.Key+`":`+a.Value)
	}
	return []byte("{" + strings.Join(parts, ",") + "}")
}

// EventAttrRaw returns the raw JSON value of one attribute ("" if absent).
func EventAttrRaw(attrs []types.EventAttr, key string) []byte {
	for _, a := range attrs {
		if a.Key == key {
			return []byte(a.Value)
		}
	}
	return nil
}
