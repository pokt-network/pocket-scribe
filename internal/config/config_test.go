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
