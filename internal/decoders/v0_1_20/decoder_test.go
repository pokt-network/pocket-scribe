package v0_1_20

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
)

var _ decoders.Decoder = Decoder{}

func TestVersion(t *testing.T) {
	if got := (Decoder{}).Version(); got != "v0_1_20" {
		t.Fatalf("Version() = %q, want v0_1_20", got)
	}
}

func TestDecodeBlockHeaderDelegates(t *testing.T) {
	if _, err := (Decoder{}).DecodeBlockHeader([]byte{0x0a, 0x01}); err == nil {
		t.Fatal("expected error decoding truncated meta bytes")
	}
}
