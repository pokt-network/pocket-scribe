package types

// EventAttr is one ABCI event attribute exactly as captured on chain. For
// typed (proto-named) events the SDK emits key = proto field name and value =
// RAW JSON (int64s quoted, enums by NAME, messages as JSON objects); legacy
// events (transfer, coin_spent, …) use plain strings.
type EventAttr struct {
	Key   string
	Value string
}
