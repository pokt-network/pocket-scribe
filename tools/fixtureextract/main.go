// fixtureextract is a one-off tool used during Task 11 fixture curation to
// decode fileplugin fixture files and produce:
//
//	(default) Report supplier activity (msgs, events, KV) as JSON to stdout.
//	(golden)  Dump raw blobs for golden tests to an output directory.
//
// Usage:
//
//	go run ./tools/fixtureextract <height> <fixture-dir>
//	go run ./tools/fixtureextract golden <height> <fixture-dir> <out-dir>
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	storetypes "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// mainnet upgrade applied heights from docs/research/poktroll-versions.md.
var mainnetUpgrades = []router.Upgrade{
	{Name: "v0.1.8", AppliedAtHeight: 78671, DecoderVersion: "v0_1_8"},
	{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
	{Name: "v0.1.17", AppliedAtHeight: 102142, DecoderVersion: "v0_1_10"}, // no v0_1_17 decoder
	{Name: "v0.1.20", AppliedAtHeight: 135297, DecoderVersion: "v0_1_20"},
	{Name: "v0.1.27", AppliedAtHeight: 247893, DecoderVersion: "v0_1_27"},
	{Name: "v0.1.28", AppliedAtHeight: 287932, DecoderVersion: "v0_1_28"},
	{Name: "v0.1.29", AppliedAtHeight: 382250, DecoderVersion: "v0_1_29"},
}

type msgStakeResult struct {
	TxIndex         int    `json:"tx_index"`
	OperatorAddress string `json:"operator_address"`
	StakeAmount     int64  `json:"stake_amount"`
	StakeDenom      string `json:"stake_denom"`
}

type eventStakedResult struct {
	TxIndex          int   `json:"tx_index"`
	SessionEndHeight int64 `json:"session_end_height"`
}

type supplierResult struct {
	MsgStake         []msgStakeResult    `json:"msg_stake,omitempty"`
	EventsStaked     []eventStakedResult `json:"events_staked,omitempty"`
	HistoryOperators []string            `json:"history_operators,omitempty"`
	SCURowsMin       int                 `json:"scu_rows_min"`
}

type fixtureResult struct {
	Height          int64           `json:"height"`
	Time            string          `json:"time"`
	Hash            string          `json:"hash"`
	ProposerAddress string          `json:"proposer_address"`
	TxCount         int             `json:"tx_count"`
	Supplier        *supplierResult `json:"supplier,omitempty"`
}

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "golden" {
		if len(os.Args) != 5 {
			fmt.Fprintln(os.Stderr, "usage: fixtureextract golden <height> <fixture-dir> <out-dir>")
			os.Exit(1)
		}
		runGolden(os.Args[2], os.Args[3], os.Args[4])
		return
	}
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: fixtureextract <height> <fixture-dir>")
		fmt.Fprintln(os.Stderr, "       fixtureextract golden <height> <fixture-dir> <out-dir>")
		os.Exit(1)
	}
	runReport(os.Args[1], os.Args[2])
}

func buildRouter() router.Router {
	reg := router.DefaultRegistry()
	r, err := router.NewStaticRouter(mainnetUpgrades, reg, "v0_1_0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "router: %v\n", err)
		os.Exit(1)
	}
	return r
}

func parseHeight(s string) int64 {
	var h int64
	if _, err := fmt.Sscanf(s, "%d", &h); err != nil {
		fmt.Fprintf(os.Stderr, "bad height: %v\n", err)
		os.Exit(1)
	}
	return h
}

func readFixture(dir string, height int64) (metaBytes, dataBytes []byte) {
	metaPath := fmt.Sprintf("%s/block-%d-meta", dir, height)
	dataPath := fmt.Sprintf("%s/block-%d-data", dir, height)
	var err error
	metaBytes, err = os.ReadFile(metaPath) //nolint:gosec
	if err != nil {
		fmt.Fprintf(os.Stderr, "read meta: %v\n", err)
		os.Exit(1)
	}
	dataBytes, err = os.ReadFile(dataPath) //nolint:gosec
	if err != nil {
		fmt.Fprintf(os.Stderr, "read data: %v\n", err)
		os.Exit(1)
	}
	return metaBytes, dataBytes
}

func runReport(heightStr, dir string) {
	height := parseHeight(heightStr)
	r := buildRouter()
	metaBytes, dataBytes := readFixture(dir, height)

	header, err := decoders.DecodeBlockHeader(metaBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decode header: %v\n", err)
		os.Exit(1)
	}

	dec, err := r.DecoderFor(height)
	if err != nil {
		fmt.Fprintf(os.Stderr, "router: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "decoder: %s\n", dec.Version())

	records, err := decoders.SplitMeta(metaBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "split meta: %v\n", err)
		os.Exit(1)
	}

	var req abci.RequestFinalizeBlock
	if err := req.Unmarshal(records[0]); err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal req: %v\n", err)
		os.Exit(1)
	}
	var resp abci.ResponseFinalizeBlock
	if err := resp.Unmarshal(records[1]); err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal resp: %v\n", err)
		os.Exit(1)
	}

	result := fixtureResult{
		Height:          header.Height,
		Time:            header.Time.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		Hash:            header.Hash,
		ProposerAddress: header.ProposerAddress,
		TxCount:         len(req.Txs),
	}

	sup := &supplierResult{}

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
			fmt.Fprintf(os.Stderr, "tx %d unmarshal: %v\n", txIdx, err)
			continue
		}
		if tx.Body == nil {
			continue
		}
		for _, anyMsg := range tx.Body.Messages {
			msg, err := dec.DecodeSupplierMsg(anyMsg.TypeUrl, anyMsg.Value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "tx %d msg decode err: %v\n", txIdx, err)
				continue
			}
			if msg == nil {
				continue
			}
			if msg.Stake != nil {
				sup.MsgStake = append(sup.MsgStake, msgStakeResult{
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
				fmt.Fprintf(os.Stderr, "event decode err (tx %d, type %s): %v\n", txIdx, ev.Type, err)
				continue
			}
			if evResult == nil {
				continue
			}
			if evResult.Staked != nil {
				sup.EventsStaked = append(sup.EventsStaked, eventStakedResult{
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
			fmt.Fprintf(os.Stderr, "kv record %d: %v\n", kvIdx, err)
			break
		}
		var kv storetypes.StoreKVPair
		if err := kv.Unmarshal(payload); err != nil {
			fmt.Fprintf(os.Stderr, "kv unmarshal %d: %v\n", kvIdx, err)
			rest = rest[consumed:]
			kvIdx++
			continue
		}
		if kv.StoreKey == "supplier" {
			kvRec, err := dec.DecodeSupplierKV(kv.Key, kv.Value, kv.Delete)
			if err != nil {
				fmt.Fprintf(os.Stderr, "supplier kv decode %d: %v\n", kvIdx, err)
			} else if kvRec != nil {
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

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "encode: %v\n", err)
		os.Exit(1)
	}
}

// runGolden extracts raw blob files used in golden tests.
func runGolden(heightStr, dir, outDir string) {
	height := parseHeight(heightStr)
	r := buildRouter()
	metaBytes, dataBytes := readFixture(dir, height)

	dec, err := r.DecoderFor(height)
	if err != nil {
		fmt.Fprintf(os.Stderr, "router: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "decoder: %s\n", dec.Version())

	records, err := decoders.SplitMeta(metaBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "split meta: %v\n", err)
		os.Exit(1)
	}

	var req abci.RequestFinalizeBlock
	if err := req.Unmarshal(records[0]); err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal req: %v\n", err)
		os.Exit(1)
	}
	var resp abci.ResponseFinalizeBlock
	if err := resp.Unmarshal(records[1]); err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal resp: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil { //nolint:gosec
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	// --- Extract first MsgStakeSupplier Any.value ---
	for txIdx, txBytes := range req.Txs {
		if txIdx >= len(resp.TxResults) || resp.TxResults[txIdx].Code != 0 {
			continue
		}
		var tx sdktx.Tx
		if err := tx.Unmarshal(txBytes); err != nil {
			continue
		}
		if tx.Body == nil {
			continue
		}
		for _, anyMsg := range tx.Body.Messages {
			if anyMsg.TypeUrl == "/pocket.supplier.MsgStakeSupplier" {
				writeBlobOrDie(outDir+"/msg_stake.bin", anyMsg.Value)
				fmt.Fprintf(os.Stderr, "msg_stake.bin: %d bytes (tx %d)\n", len(anyMsg.Value), txIdx)
				goto foundMsg
			}
		}
	}
foundMsg:

	// --- Extract first EventSupplierStaked attribute set ---
	for txIdx, txResult := range resp.TxResults {
		if txResult.Code != 0 {
			continue
		}
		for _, ev := range txResult.Events {
			if ev.Type == "pocket.supplier.EventSupplierStaked" {
				// Build the JSON doc the decoder builds from attrs (skip mode/msg_index).
				attrs := make([]types.EventAttr, 0, len(ev.Attributes))
				for _, a := range ev.Attributes {
					attrs = append(attrs, types.EventAttr{Key: a.Key, Value: a.Value})
				}
				doc := decoders.EventAttrsJSON(attrs)
				writeBlobOrDie(outDir+"/event_staked.json", doc)
				fmt.Fprintf(os.Stderr, "event_staked.json: %d bytes (tx %d)\n", len(doc), txIdx)
				goto foundEvent
			}
		}
	}
foundEvent:

	// --- Extract first Supplier/operator_address/ KV value and first SCU KV value ---
	supplierKVDone, scuKVDone := false, false
	rest := dataBytes
	kvIdx := 0
	for len(rest) > 0 && (!supplierKVDone || !scuKVDone) {
		payload, consumed, err := decoders.ReadDelimited(rest)
		if err != nil {
			fmt.Fprintf(os.Stderr, "kv record %d: %v\n", kvIdx, err)
			break
		}
		var kv storetypes.StoreKVPair
		if err := kv.Unmarshal(payload); err != nil {
			rest = rest[consumed:]
			kvIdx++
			continue
		}
		if kv.StoreKey == "supplier" && !kv.Delete {
			switch decoders.ClassifySupplierKey(kv.Key) {
			case decoders.SupplierKeyRecord:
				if !supplierKVDone {
					writeBlobOrDie(outDir+"/supplier_kv.bin", kv.Value)
					fmt.Fprintf(os.Stderr, "supplier_kv.bin: %d bytes (kv %d)\n", len(kv.Value), kvIdx)
					supplierKVDone = true
				}
			case decoders.SupplierKeySCURecord:
				if !scuKVDone {
					writeBlobOrDie(outDir+"/scu_kv.bin", kv.Value)
					fmt.Fprintf(os.Stderr, "scu_kv.bin: %d bytes (kv %d)\n", len(kv.Value), kvIdx)
					scuKVDone = true
				}
			}
		}
		rest = rest[consumed:]
		kvIdx++
	}

	if !supplierKVDone {
		fmt.Fprintln(os.Stderr, "WARNING: no Supplier/operator_address/ KV found")
	}
	if !scuKVDone {
		fmt.Fprintln(os.Stderr, "WARNING: no ServiceConfigUpdate/service_id/ KV found")
	}
	fmt.Fprintln(os.Stderr, "done")
}

func writeBlobOrDie(path string, data []byte) {
	if err := os.WriteFile(path, data, 0o644); err != nil { //nolint:gosec
		fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
		os.Exit(1)
	}
}
