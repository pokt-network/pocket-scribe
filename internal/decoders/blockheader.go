package decoders

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	abci "github.com/cometbft/cometbft/abci/types"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// maxRecordSize bounds the first length-delimited record we read from a meta
// file. A real RequestFinalizeBlock is a few KiB; 64 MiB is a generous ceiling
// that still rejects a corrupt/garbage length prefix before allocating.
const maxRecordSize = 64 << 20

// readDelimited reads ONE length-delimited protobuf record from the front of buf
// using the framing the Cosmos SDK FilePlugin writes: a base-128 uvarint length
// prefix (binary.PutUvarint) followed by exactly that many payload bytes. It
// returns the payload (a sub-slice of buf, not copied) and the bytes consumed
// (prefix + payload). The block header is the FIRST record of `block-{H}-meta`.
func readDelimited(buf []byte) (payload []byte, consumed int, err error) {
	length, n := binary.Uvarint(buf)
	if n == 0 {
		return nil, 0, errors.New("decoders: meta record truncated reading length prefix")
	}
	if n < 0 {
		return nil, 0, errors.New("decoders: meta record length prefix overflows 64 bits")
	}
	if length > maxRecordSize {
		return nil, 0, fmt.Errorf("decoders: meta record length %d exceeds max %d", length, maxRecordSize)
	}
	end := n + int(length)
	if end > len(buf) {
		return nil, 0, fmt.Errorf("decoders: meta record truncated: need %d bytes, have %d", end, len(buf))
	}
	return buf[n:end], end, nil
}

// DecodeBlockHeader parses the FIRST length-delimited record of a FilePlugin
// `block-{H}-meta` file as a cometbft abci RequestFinalizeBlock and projects the
// consensus-header fields PocketScribe needs (invariant #1: height + time are the
// queryable axis). The block header is version-invariant across every poktroll
// release — it is cometbft ABCI 2.0, byte-identical for all 32 vendored versions
// — so every versioned decoder delegates here rather than reimplementing it.
//
// The abci import resolves to the pokt-network cometbft fork via the go.mod
// replace directive. Hash and ProposerAddress are hex-encoded (lowercase) to
// match the `block` table TEXT columns.
func DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	payload, _, err := readDelimited(metaBytes)
	if err != nil {
		return nil, err
	}

	var req abci.RequestFinalizeBlock
	// Unmarshal is the gogo-generated method on the cometbft type. Do NOT use
	// google.golang.org/protobuf here: these are gogoproto messages with a
	// stdtime Time field and the two runtimes are not interchangeable.
	if err := req.Unmarshal(payload); err != nil {
		return nil, fmt.Errorf("decoders: decode RequestFinalizeBlock: %w", err)
	}
	if req.Height <= 0 {
		return nil, fmt.Errorf("decoders: decoded non-positive height %d", req.Height)
	}

	return &types.BlockHeader{
		Height:          req.Height,
		Time:            req.Time,
		Hash:            hex.EncodeToString(req.Hash),
		ProposerAddress: hex.EncodeToString(req.ProposerAddress),
		TxCount:         len(req.Txs),
	}, nil
}
