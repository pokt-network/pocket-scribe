//go:build ignore

// decode295476: one-shot helper to decode block-295476 fixture files and
// output the expected.json for the unbonding fixture. Not compiled into the
// binary; run with: go run tools/decode295476/main.go
package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	abci "github.com/cometbft/cometbft/abci/types"
	storetypes "cosmossdk.io/store/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"

	v0_1_28 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_28"
	"github.com/pokt-network/pocketscribe/internal/types"
)

func readDelimited(buf []byte) ([]byte, int, error) {
	length, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil, 0, fmt.Errorf("bad uvarint n=%d", n)
	}
	end := n + int(length)
	if end > len(buf) {
		return nil, 0, fmt.Errorf("truncated: need %d, have %d", end, len(buf))
	}
	return buf[n:end], end, nil
}

func main() {
	dec := v0_1_28.Decoder{}
	metaPath := "/tmp/pse-unbonding/block-295476-meta"
	dataPath := "/tmp/pse-unbonding/block-295476-data"

	meta, err := os.ReadFile(metaPath)
	if err != nil {
		panic("read meta: " + err.Error())
	}

	payload, consumed, err := readDelimited(meta)
	if err != nil {
		panic("readDelimited meta: " + err.Error())
	}
	var req abci.RequestFinalizeBlock
	if err := req.Unmarshal(payload); err != nil {
		panic("unmarshal req: " + err.Error())
	}

	remaining := meta[consumed:]
	resp2, _, err := readDelimited(remaining)
	if err != nil {
		panic("readDelimited resp: " + err.Error())
	}
	var fbr abci.ResponseFinalizeBlock
	if err := fbr.Unmarshal(resp2); err != nil {
		panic("unmarshal fbr: " + err.Error())
	}

	// Decode MsgUnstakeSupplier from txs
	type MsgUnstakeRow struct {
		TxIndex         int    `json:"tx_index"`
		OperatorAddress string `json:"operator_address"`
	}
	var msgUnstake []MsgUnstakeRow

	for i, txBytes := range req.Txs {
		var tx sdktx.Tx
		if err := tx.Unmarshal(txBytes); err != nil {
			continue
		}
		if tx.Body == nil {
			continue
		}
		for _, msg := range tx.Body.Messages {
			decoded, err := dec.DecodeSupplierMsg(msg.TypeUrl, msg.Value)
			if err != nil || decoded == nil {
				continue
			}
			if decoded.Unstake != nil {
				msgUnstake = append(msgUnstake, MsgUnstakeRow{
					TxIndex:         i,
					OperatorAddress: decoded.Unstake.OperatorAddress,
				})
			}
		}
	}

	// Decode EventSupplierUnbondingBegin from tx results
	type EventUnbondingBeginRow struct {
		TxIndex            int   `json:"tx_index"`
		SessionEndHeight   int64 `json:"session_end_height"`
		UnbondingEndHeight int64 `json:"unbonding_end_height"`
	}
	var evUnbonding []EventUnbondingBeginRow

	for i, txResult := range fbr.TxResults {
		for _, ev := range txResult.Events {
			if !strings.Contains(ev.Type, "UnbondingBegin") {
				continue
			}
			attrs := make([]types.EventAttr, 0, len(ev.Attributes))
			for _, a := range ev.Attributes {
				if a.Key == "msg_index" || a.Key == "mode" {
					continue
				}
				attrs = append(attrs, types.EventAttr{Key: a.Key, Value: a.Value})
			}
			decoded, err := dec.DecodeSupplierEvent(ev.Type, attrs)
			if err != nil || decoded == nil || decoded.UnbondingBegin == nil {
				fmt.Fprintf(os.Stderr, "decode event err tx[%d]: %v\n", i, err)
				continue
			}
			ub := decoded.UnbondingBegin
			evUnbonding = append(evUnbonding, EventUnbondingBeginRow{
				TxIndex:            i,
				SessionEndHeight:   ub.SessionEndHeight,
				UnbondingEndHeight: ub.UnbondingEndHeight,
			})
		}
	}

	// Decode KV pairs for supplier history and SCU count
	data, err := os.ReadFile(dataPath)
	if err != nil {
		panic("read data: " + err.Error())
	}
	offset := 0
	seenOps := map[string]bool{}
	var historyOps []string
	scuCount := 0

	for offset < len(data) {
		payload2, consumed2, err := readDelimited(data[offset:])
		if err != nil {
			break
		}
		offset += consumed2

		var kv storetypes.StoreKVPair
		if err := kv.Unmarshal(payload2); err != nil {
			continue
		}
		decoded, err := dec.DecodeSupplierKV(kv.Key, kv.Value, kv.Delete)
		if err != nil || decoded == nil {
			continue
		}
		if decoded.Supplier != nil && !seenOps[decoded.Supplier.OperatorAddress] {
			seenOps[decoded.Supplier.OperatorAddress] = true
			historyOps = append(historyOps, decoded.Supplier.OperatorAddress)
		}
		if decoded.ServiceConfigUpdate != nil {
			scuCount++
		}
	}
	sort.Strings(historyOps)

	out := map[string]any{
		"height":           req.Height,
		"time":             req.Time.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		"hash":             hex.EncodeToString(req.Hash),
		"proposer_address": hex.EncodeToString(req.ProposerAddress),
		"tx_count":         len(req.Txs),
		"supplier": map[string]any{
			"msg_unstake":            msgUnstake,
			"events_unbonding_begin": evUnbonding,
			"history_operators":      historyOps,
			"scu_rows_min":           scuCount,
		},
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}
