package config

import "testing"

func TestLoadMainnet(t *testing.T) {
	cfg, err := Load("../../configs/networks/mainnet.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Network.ID != "pocket-mainnet" {
		t.Errorf("Network.ID = %q, want pocket-mainnet", cfg.Network.ID)
	}
	if cfg.Network.ChainID != "pocket" {
		t.Errorf("Network.ChainID = %q, want pocket", cfg.Network.ChainID)
	}
	if cfg.Network.GenesisDecoderVersion != "v0_1_0" {
		t.Errorf("GenesisDecoderVersion = %q, want v0_1_0", cfg.Network.GenesisDecoderVersion)
	}
	if cfg.Network.GenesisHeight != 1 {
		t.Errorf("GenesisHeight = %d, want 1", cfg.Network.GenesisHeight)
	}
	if len(cfg.Endpoints.RPC) == 0 {
		t.Error("expected at least one RPC endpoint")
	}
	if len(cfg.Network.UpgradeNames) == 0 {
		t.Error("expected non-empty upgrade_names for mainnet")
	}
	found := false
	for _, n := range cfg.Network.UpgradeNames {
		if n == "v0.1.30" {
			found = true
			break
		}
	}
	if !found {
		t.Error("upgrade_names must contain v0.1.30")
	}
	if cfg.Endpoints.LCD[0] != "https://sauron-api.infra.pocket.network" {
		t.Errorf("Endpoints.LCD[0] = %q, want https://sauron-api.infra.pocket.network", cfg.Endpoints.LCD[0])
	}
}

func TestLoadLocalnetDynamicGenesisTime(t *testing.T) {
	cfg, err := Load("../../configs/networks/localnet.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// genesis_time is the literal string "dynamic" on localnet — must not error.
	if cfg.Network.GenesisTime != "dynamic" {
		t.Errorf("GenesisTime = %q, want dynamic", cfg.Network.GenesisTime)
	}
	if cfg.Network.GenesisDecoderVersion != "v0_1_33" {
		t.Errorf("GenesisDecoderVersion = %q, want v0_1_33", cfg.Network.GenesisDecoderVersion)
	}
}

func TestLoadValidatesRequiredFields(t *testing.T) {
	if _, err := Load("testdata/missing_chain_id.yaml"); err == nil {
		t.Fatal("expected validation error for missing chain_id")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("testdata/does_not_exist.yaml"); err == nil {
		t.Fatal("expected error for missing file")
	}
}

// TestValidate_AllMissingFields covers each required-field check in validate
// independently, so the error-path branches all reach 100%.

func TestValidate_MissingNetworkID(t *testing.T) {
	_, err := Load("testdata/missing_network_id.yaml")
	if err == nil {
		t.Fatal("expected validation error for missing network.id")
	}
}

func TestValidate_MissingGenesisDecoderVersion(t *testing.T) {
	_, err := Load("testdata/missing_genesis_decoder_version.yaml")
	if err == nil {
		t.Fatal("expected validation error for missing genesis_decoder_version")
	}
}

func TestValidate_ZeroGenesisHeight(t *testing.T) {
	_, err := Load("testdata/zero_genesis_height.yaml")
	if err == nil {
		t.Fatal("expected validation error for genesis_height=0 (must be >= 1)")
	}
}

func TestValidate_MissingRPC(t *testing.T) {
	_, err := Load("testdata/missing_rpc.yaml")
	if err == nil {
		t.Fatal("expected validation error for empty endpoints.rpc")
	}
}
