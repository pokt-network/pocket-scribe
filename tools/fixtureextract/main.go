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

	storetypes "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/fixturereport"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/types"
)

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
	r, err := router.NewStaticRouter(fixturereport.MainnetUpgrades(), reg, "v0_1_0")
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
	metaBytes, dataBytes := readFixture(dir, height)
	r := buildRouter()
	res, err := fixturereport.Report(r, metaBytes, dataBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "report: %v\n", err)
		os.Exit(1)
	}
	if res.Height != height {
		fmt.Fprintf(os.Stderr, "WARNING: decoded height %d != filename height %d\n", res.Height, height)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
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
