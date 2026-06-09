package v0_1_30

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
)

// Decoder must satisfy the shared decoders.Decoder interface.
var _ decoders.Decoder = Decoder{}

func TestVersion(t *testing.T) {
	if got := (Decoder{}).Version(); got != "v0_1_30" {
		t.Fatalf("Version() = %q, want v0_1_30", got)
	}
}

func TestDecodeBlockHeaderDelegates(t *testing.T) {
	// Truncated meta bytes must surface the shared decoder's error through the
	// delegation (proves the method is wired without duplicating the fixture test).
	if _, err := (Decoder{}).DecodeBlockHeader([]byte{0x0a, 0x01}); err == nil {
		t.Fatal("expected error decoding truncated meta bytes")
	}
}
