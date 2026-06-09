package types

import "time"

// BlockHeader is the canonical consensus-header projection written to the
// `block` table (one row per height). Height and Time are the queryable axis
// mandated by invariant #1 (chain consensus header, never indexer wall-clock).
// Hash and ProposerAddress are hex-encoded lowercase to match the table's TEXT
// columns. The block header is version-invariant across poktroll releases, so it
// carries no proto_version; chain_id is injected from network config (it is not
// present in the per-block ABCI header, and the `block` table has no chain_id
// column).
type BlockHeader struct {
	Height          int64     // block.height (BIGINT PRIMARY KEY)
	Time            time.Time // block.time (TIMESTAMPTZ) — consensus header time
	Hash            string    // block.hash (TEXT, hex lowercase)
	ProposerAddress string    // block.proposer_address (TEXT, hex lowercase; 20-byte consensus addr)
	TxCount         int       // block.tx_count (INTEGER) = len(RequestFinalizeBlock.Txs)
}
