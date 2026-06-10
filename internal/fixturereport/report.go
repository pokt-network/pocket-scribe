package fixturereport

import (
	"fmt"
	"sort"
	"strings"

	storetypes "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// MsgStakeResult holds a decoded MsgStakeSupplier from a single transaction.
type MsgStakeResult struct {
	TxIndex         int    `json:"tx_index"`
	OperatorAddress string `json:"operator_address"`
	StakeAmount     int64  `json:"stake_amount"`
	StakeDenom      string `json:"stake_denom"`
}

// EventStakedResult holds a decoded EventSupplierStaked from a single transaction.
type EventStakedResult struct {
	TxIndex          int   `json:"tx_index"`
	SessionEndHeight int64 `json:"session_end_height"`
}

// SupplierResult summarises all supplier activity found in the block.
type SupplierResult struct {
	MsgStake         []MsgStakeResult    `json:"msg_stake,omitempty"`
	EventsStaked     []EventStakedResult `json:"events_staked,omitempty"`
	HistoryOperators []string            `json:"history_operators,omitempty"`
	SCURowsMin       int                 `json:"scu_rows_min"`
}

// Result is the decoded summary of one captured FilePlugin block fixture.
// JSON tags match the tool-local types that generated the existing expected.json
// files — they must never be changed without regenerating the corpus.
type Result struct {
	Height          int64           `json:"height"`
	Time            string          `json:"time"`
	Hash            string          `json:"hash"`
	ProposerAddress string          `json:"proposer_address"`
	TxCount         int             `json:"tx_count"`
	Supplier        *SupplierResult `json:"supplier,omitempty"`
}

// Report decodes one captured block. The height is taken from the decoded
// header (the caller's filename is not trusted). Per-item decode failures
// inside txs/events/KV are skipped (same behavior the curation tool always
// had); structural failures (header, meta framing) return an error.
func Report(r router.Router, metaBytes, dataBytes []byte) (*Result, error) {
	header, err := decoders.DecodeBlockHeader(metaBytes)
	if err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}

	dec, err := r.DecoderFor(header.Height)
	if err != nil {
		return nil, fmt.Errorf("router: %w", err)
	}

	records, err := decoders.SplitMeta(metaBytes)
	if err != nil {
		return nil, fmt.Errorf("split meta: %w", err)
	}

	var req abci.RequestFinalizeBlock
	if err := req.Unmarshal(records[0]); err != nil {
		return nil, fmt.Errorf("unmarshal req: %w", err)
	}
	var resp abci.ResponseFinalizeBlock
	if err := resp.Unmarshal(records[1]); err != nil {
		return nil, fmt.Errorf("unmarshal resp: %w", err)
	}

	result := &Result{
		Height:          header.Height,
		Time:            header.Time.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		Hash:            header.Hash,
		ProposerAddress: header.ProposerAddress,
		TxCount:         len(req.Txs),
	}

	sup := &SupplierResult{}

	// Decode txs for MsgStakeSupplier/MsgUnstakeSupplier.
	for txIdx, txBytes := range req.Txs {
		if txIdx >= len(resp.TxResults) {
			continue
		}
		if resp.TxResults[txIdx].Code != 0 {
			continue // failed txs change no state (decision 7)
		}
		var tx sdktx.Tx
		if err := tx.Unmarshal(txBytes); err != nil {
			continue
		}
		if tx.Body == nil {
			continue
		}
		for _, anyMsg := range tx.Body.Messages {
			msg, err := dec.DecodeSupplierMsg(anyMsg.TypeUrl, anyMsg.Value)
			if err != nil {
				continue
			}
			if msg == nil {
				continue
			}
			if msg.Stake != nil {
				sup.MsgStake = append(sup.MsgStake, MsgStakeResult{
					TxIndex:         txIdx,
					OperatorAddress: msg.Stake.OperatorAddress,
					StakeAmount:     msg.Stake.StakeAmount,
					StakeDenom:      msg.Stake.StakeDenom,
				})
			}
		}
	}

	// Decode typed events for supplier module.
	for txIdx, txResult := range resp.TxResults {
		if txResult.Code != 0 {
			continue
		}
		for _, ev := range txResult.Events {
			if !strings.HasPrefix(ev.Type, "pocket.supplier.") {
				continue
			}
			attrs := make([]types.EventAttr, 0, len(ev.Attributes))
			for _, a := range ev.Attributes {
				attrs = append(attrs, types.EventAttr{Key: a.Key, Value: a.Value})
			}
			evResult, err := dec.DecodeSupplierEvent(ev.Type, attrs)
			if err != nil {
				continue
			}
			if evResult == nil {
				continue
			}
			if evResult.Staked != nil {
				sup.EventsStaked = append(sup.EventsStaked, EventStakedResult{
					TxIndex:          txIdx,
					SessionEndHeight: evResult.Staked.SessionEndHeight,
				})
			}
		}
	}

	// Decode KV for supplier store.
	scuCount := 0
	supplierOps := make(map[string]bool)
	rest := dataBytes
	kvIdx := 0
	for len(rest) > 0 {
		payload, consumed, err := decoders.ReadDelimited(rest)
		if err != nil {
			break
		}
		var kv storetypes.StoreKVPair
		if err := kv.Unmarshal(payload); err != nil {
			rest = rest[consumed:]
			kvIdx++
			continue
		}
		if kv.StoreKey == "supplier" {
			kvRec, err := dec.DecodeSupplierKV(kv.Key, kv.Value, kv.Delete)
			if err == nil && kvRec != nil {
				if kvRec.Supplier != nil {
					supplierOps[kvRec.Supplier.OperatorAddress] = true
				}
				if kvRec.ServiceConfigUpdate != nil && !kvRec.ServiceConfigUpdate.Deleted {
					scuCount++
				}
			}
		}
		rest = rest[consumed:]
		kvIdx++
	}

	for op := range supplierOps {
		sup.HistoryOperators = append(sup.HistoryOperators, op)
	}
	sort.Strings(sup.HistoryOperators)
	sup.SCURowsMin = scuCount

	if len(sup.MsgStake) > 0 || len(sup.EventsStaked) > 0 || len(sup.HistoryOperators) > 0 || sup.SCURowsMin > 0 {
		result.Supplier = sup
	}

	return result, nil
}
