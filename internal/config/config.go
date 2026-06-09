package config // package comment lives in internal/config/doc.go (do not repeat it — revive package-comments)

import (
	"fmt"
	"reflect"
	"time"

	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// timeToStringHookFunc returns a decode hook that converts time.Time values to
// their RFC3339Nano string representation. The YAML parser may auto-convert
// timestamp literals (e.g. the mainnet genesis_time) into time.Time before
// mapstructure sees them; this hook round-trips them back to string so our
// GenesisTime field stays a plain string (localnet uses the literal "dynamic").
func timeToStringHookFunc() mapstructure.DecodeHookFunc {
	return func(from reflect.Type, to reflect.Type, data any) (any, error) {
		if from == reflect.TypeOf(time.Time{}) && to == reflect.TypeOf("") {
			return data.(time.Time).Format(time.RFC3339Nano), nil
		}
		return data, nil
	}
}

// Load reads and validates a network config file at path.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		timeToStringHookFunc(),
		mapstructure.StringToTimeDurationHookFunc(),
	))); err != nil {
		return nil, fmt.Errorf("unmarshal config %s: %w", path, err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	var missing []string
	if c.Network.ID == "" {
		missing = append(missing, "network.id")
	}
	if c.Network.ChainID == "" {
		missing = append(missing, "network.chain_id")
	}
	if c.Network.GenesisDecoderVersion == "" {
		missing = append(missing, "network.genesis_decoder_version")
	}
	if c.Network.GenesisHeight < 1 {
		missing = append(missing, "network.genesis_height (must be >= 1)")
	}
	if len(c.Endpoints.RPC) == 0 {
		missing = append(missing, "endpoints.rpc (need at least one)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid fields: %v", missing)
	}
	return nil
}
