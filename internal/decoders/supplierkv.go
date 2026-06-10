package decoders

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// SupplierKeyKind classifies a supplier-store KV key (spike §4d table).
type SupplierKeyKind int

const (
	// SupplierKeyIgnore indicates an index layout whose value is primary-key
	// pointer bytes, params ("p_supplier"), or an unbonding-height marker —
	// NOT a proto; skip.
	SupplierKeyIgnore SupplierKeyKind = iota
	// SupplierKeyRecord indicates a Supplier/operator_address/<addr>/ key
	// whose value is a pocket.shared.Supplier proto.
	SupplierKeyRecord
	// SupplierKeySCURecord indicates a
	// ServiceConfigUpdate/service_id/<svc>/<actH:BE8>/<addr>/ key whose value
	// is a full pocket.shared.ServiceConfigUpdate proto (the PRIMARY layout).
	SupplierKeySCURecord
)

const (
	supplierRecordPrefix = "Supplier/operator_address/"
	scuPrimaryPrefix     = "ServiceConfigUpdate/service_id/"
)

// ClassifySupplierKey discriminates supplier-store keys. Only two layouts
// carry proto values; everything else is skipped (invariant 3: decoding an
// index pointer as a Supplier would write garbage snapshots).
func ClassifySupplierKey(key []byte) SupplierKeyKind {
	switch {
	case bytes.HasPrefix(key, []byte(supplierRecordPrefix)):
		return SupplierKeyRecord
	case bytes.HasPrefix(key, []byte(scuPrimaryPrefix)):
		return SupplierKeySCURecord
	default:
		return SupplierKeyIgnore
	}
}

// ParseSCUPrimaryKey extracts (serviceID, activationHeight, operatorAddress)
// from ServiceConfigUpdate/service_id/<svc>/<actH:8-byte big-endian>/<addr>/
// (all segments end with '/'; heights are 8-byte big-endian — spike §4d).
// Needed when a deleted record has an empty value.
func ParseSCUPrimaryKey(key []byte) (serviceID string, activationHeight int64, operator string, err error) {
	rest := bytes.TrimPrefix(key, []byte(scuPrimaryPrefix))
	i := bytes.IndexByte(rest, '/')
	if i < 0 {
		return "", 0, "", fmt.Errorf("malformed SCU key (no service segment): %q", key)
	}
	serviceID = string(rest[:i])
	rest = rest[i+1:]
	if len(rest) < 9 || rest[8] != '/' {
		return "", 0, "", fmt.Errorf("malformed SCU key (no height segment): %q", key)
	}
	activationHeight = int64(binary.BigEndian.Uint64(rest[:8]))
	rest = rest[9:]
	if j := bytes.IndexByte(rest, '/'); j > 0 {
		operator = string(rest[:j])
	} else {
		operator = string(bytes.TrimSuffix(rest, []byte("/")))
	}
	if operator == "" {
		return "", 0, "", fmt.Errorf("malformed SCU key (no operator segment): %q", key)
	}
	return serviceID, activationHeight, operator, nil
}
