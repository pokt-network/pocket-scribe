package decoders

import (
	"bytes"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/types"
)

func TestEventAttrsJSONSkipsBookkeepingAndSplicesRaw(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"135840"`},
		{Key: "mode", Value: "EndBlock"},
		{Key: "supplier", Value: `{"operator_address":"pokt16ar"}`},
		{Key: "msg_index", Value: "0"},
	}
	got := EventAttrsJSON(attrs)
	want := `{"session_end_height":"135840","supplier":{"operator_address":"pokt16ar"}}`
	if string(got) != want {
		t.Fatalf("EventAttrsJSON = %s, want %s", got, want)
	}
	if raw := EventAttrRaw(attrs, "supplier"); !bytes.Equal(raw, []byte(`{"operator_address":"pokt16ar"}`)) {
		t.Fatalf("EventAttrRaw = %s", raw)
	}
	if raw := EventAttrRaw(attrs, "absent"); raw != nil {
		t.Fatalf("EventAttrRaw(absent) = %s, want nil", raw)
	}
}
