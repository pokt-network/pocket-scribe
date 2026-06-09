package config

// Config is a parsed network config file (configs/networks/<name>.yaml).
type Config struct {
	Network   Network   `mapstructure:"network"`
	Endpoints Endpoints `mapstructure:"endpoints"`
}

// Network describes the chain a PocketScribe deployment indexes. Field names
// mirror the on-disk YAML exactly (ADR-018: config is the source of truth).
type Network struct {
	ID                    string   `mapstructure:"id"`
	ChainID               string   `mapstructure:"chain_id"`
	DisplayName           string   `mapstructure:"display_name"`
	GenesisHeight         int64    `mapstructure:"genesis_height"`
	GenesisTime           string   `mapstructure:"genesis_time"`            // RFC3339 or the literal "dynamic" (localnet)
	GenesisDecoderVersion string   `mapstructure:"genesis_decoder_version"` // underscored, e.g. "v0_1_0"
	StartHeight           *int64   `mapstructure:"start_height"`            // optional partial-history bootstrap (ADR-019)
	UpgradeNames          []string `mapstructure:"upgrade_names"`           // x/upgrade plan names ps sync-upgrades queries (ADR-018)
}

// Endpoints lists the chain access endpoints. Each is a list; any may be empty.
type Endpoints struct {
	RPC  []string `mapstructure:"rpc"`
	LCD  []string `mapstructure:"lcd"`
	GRPC []string `mapstructure:"grpc"`
}
