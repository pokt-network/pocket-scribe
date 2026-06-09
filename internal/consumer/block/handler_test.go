package block

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_30 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30"
	"github.com/pokt-network/pocketscribe/internal/types"
)

type stubRouter struct{ d decoders.Decoder }

func (s stubRouter) DecoderFor(int64) (decoders.Decoder, error) { return s.d, nil }

type fakeInserter struct{ got *types.BlockHeader }

func (f *fakeInserter) InsertBlock(_ context.Context, _ pgx.Tx, h *types.BlockHeader) error {
	f.got = h
	return nil
}

func TestHandleDecodesAndInserts(t *testing.T) {
	raw, err := os.ReadFile("../../decoders/testdata/block-190974-meta")
	if err != nil {
		t.Fatal(err)
	}
	fi := &fakeInserter{}
	h := New(stubRouter{v0_1_30.Decoder{}}, fi)
	if err := h.Handle(context.Background(), nil, consumer.Message{Height: 190974, Data: raw}); err != nil {
		t.Fatal(err)
	}
	if fi.got == nil || fi.got.Height != 190974 {
		t.Fatalf("inserter got %+v", fi.got)
	}
}
