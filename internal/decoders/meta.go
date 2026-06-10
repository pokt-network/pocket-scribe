package decoders

import (
	"fmt"

	storetypes "cosmossdk.io/store/types"
)

// metaRecordCount is the ADR-027 contract: block-{H}-meta is EXACTLY three
// uvarint-delimited gogo records — RequestFinalizeBlock, ResponseFinalizeBlock,
// ResponseCommit (empirically 0 bytes).
const metaRecordCount = 3

// SplitMeta splits a block-{H}-meta payload into its three records using the
// same uvarint framing as DecodeBlockHeader. The sidecar fan-out uses records
// [0] and [1]; record [2] is validated but unused.
func SplitMeta(metaBytes []byte) ([][]byte, error) {
	records := make([][]byte, 0, metaRecordCount)
	rest := metaBytes
	for len(rest) > 0 {
		payload, consumed, err := readDelimited(rest)
		if err != nil {
			return nil, fmt.Errorf("meta record %d: %w", len(records), err)
		}
		records = append(records, payload)
		rest = rest[consumed:]
	}
	if len(records) != metaRecordCount {
		return nil, fmt.Errorf("meta has %d records, want %d (ADR-027)", len(records), metaRecordCount)
	}
	return records, nil
}

// ReadDelimited is the exported thin wrapper around the unexported readDelimited
// framing reader, for use by the sidecar fan-out (bootstrap.go). It reads ONE
// uvarint-delimited record from the front of buf and returns the payload and the
// total bytes consumed (length prefix + payload). DRY: the framing is defined
// once in blockheader.go.
func ReadDelimited(buf []byte) ([]byte, int, error) {
	return readDelimited(buf)
}

// StoreKeyOf extracts only the store_key (field 1, string) of a
// cosmos.store.v1beta1.StoreKVPair record without a full unmarshal dependency
// here: unmarshal via cosmossdk.io/store/types (already in the module graph).
func StoreKeyOf(record []byte) (string, error) {
	var kv storetypes.StoreKVPair
	if err := kv.Unmarshal(record); err != nil {
		return "", fmt.Errorf("StoreKVPair: %w", err)
	}
	return kv.StoreKey, nil
}
