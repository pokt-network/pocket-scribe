package decoders

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cosmos/gogoproto/jsonpb"
	"github.com/cosmos/gogoproto/proto"
)

// marshalJSONPB renders a gogo message as JSON with proto field names
// (OrigName) — the same convention the SDK uses when emitting typed-event
// attributes, so stored JSONB is consistent across sources.
func marshalJSONPB(m proto.Message) ([]byte, error) {
	var b bytes.Buffer
	mr := jsonpb.Marshaler{OrigName: true, EmitDefaults: false}
	if err := mr.Marshal(&b, m); err != nil {
		return nil, fmt.Errorf("jsonpb marshal %T: %w", m, err)
	}
	return b.Bytes(), nil
}

// MarshalJSONPBSlice renders a slice of gogo messages as a JSON array.
// Returns nil for an empty slice (dehydrated-era Supplier has no services).
func MarshalJSONPBSlice[T proto.Message](items []T) ([]byte, error) {
	if len(items) == 0 {
		return nil, nil
	}
	parts := make([]string, 0, len(items))
	for _, it := range items {
		j, err := marshalJSONPB(it)
		if err != nil {
			return nil, err
		}
		parts = append(parts, string(j))
	}
	return []byte("[" + strings.Join(parts, ",") + "]"), nil
}

// UnmarshalEventJSON validates+decodes a rebuilt typed-event JSON document into
// the version's generated event type. AllowUnknownFields tolerates attributes
// added by later chain versions (they are preserved in the raw JSONB anyway).
// REQUIRES the central enum registration (enums.go): jsonpb resolves enum
// NAMES via proto.EnumValueMap, which stripregister removed from gen init().
func UnmarshalEventJSON(doc []byte, m proto.Message) error {
	um := jsonpb.Unmarshaler{AllowUnknownFields: true}
	if err := um.Unmarshal(bytes.NewReader(doc), m); err != nil {
		return fmt.Errorf("jsonpb unmarshal %T: %w", m, err)
	}
	return nil
}
