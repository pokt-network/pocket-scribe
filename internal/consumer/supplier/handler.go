// Package supplier implements the BatchHandler that indexes the supplier
// module: tx msgs → msg_*, typed events → event_supplier_*, KV writes →
// supplier_history + supplier_service_config_update_history. Decode is
// consumer-side via the router (ADR-008); rows record the REGISTERED decoder
// version the router returned (decision 8).
package supplier

import (
	"context"
	"fmt"
	"time"

	storetypes "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	"github.com/pokt-network/pocketscribe/internal/decoders"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/store"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// EventTypes are the supplier-module typed events this consumer subscribes to
// and persists (ADR-022 per-type subjects; tokenomics EventSupplierSlashed is
// the tokenomics consumer's job — decision 6).
var EventTypes = []string{
	"pocket.supplier.EventSupplierStaked",
	"pocket.supplier.EventSupplierUnbondingBegin",
	"pocket.supplier.EventSupplierUnbondingEnd",
	"pocket.supplier.EventSupplierUnbondingCanceled",
	"pocket.supplier.EventSupplierServiceConfigActivated",
}

// Router is the subset of router.Router this handler needs.
type Router interface {
	DecoderFor(height int64) (decoders.Decoder, error)
}

// Handler implements consumer.BatchHandler for the supplier module.
type Handler struct {
	router     Router
	versionIDs map[string]int16 // decoder_version tag → id (store.DecoderVersionIDs)
}

// New constructs the supplier handler.
func New(r Router, versionIDs map[string]int16) *Handler {
	return &Handler{router: r, versionIDs: versionIDs}
}

// ID returns the stable consumer name used as the JetStream durable and DB key.
func (h *Handler) ID() string { return "supplier" }

// FirstValidVersion is the earliest poktroll semver at which this consumer applies.
func (h *Handler) FirstValidVersion() string { return "v0.1.0" }

// FlushHeight decodes every buffered fan-out message for the height and writes
// the rows inside the runtime-managed transaction. Empty msgs (quiet height)
// writes nothing and succeeds.
//
// Partial-flush contract (ADR-024 triggers 2-3): env may be nil when the
// block-boundary fence has not arrived yet. In that case height and block_time
// are derived from msgs[0] (Pocket-Block-Time header → Message.TimeUnixNano).
// No envelope-derived rows are written. The cursor is NOT advanced — that
// remains the exclusive job of the block-boundary fence path.
func (h *Handler) FlushHeight(ctx context.Context, tx pgx.Tx, env *psv1.BlockEnvelope, msgs []consumer.Message) error {
	var height, tnano int64
	switch {
	case env != nil:
		// Block-boundary fence path: envelope carries height + block_time.
		// Empty msgs is valid (quiet height — no supplier activity at this block).
		if len(msgs) == 0 {
			return nil
		}
		height, tnano = env.Height, env.TimeUnixNano
	case len(msgs) > 0 && msgs[0].TimeUnixNano > 0:
		// Partial-flush path (ADR-024 trigger 2 size, trigger 3 time): env is nil;
		// derive height and block_time from msgs[0].
		height, tnano = msgs[0].Height, msgs[0].TimeUnixNano
	default:
		return fmt.Errorf("partial flush requires messages with Pocket-Block-Time (ADR-022 amendment)")
	}
	dec, err := h.router.DecoderFor(height)
	if err != nil {
		return err
	}
	decodedBy, ok := h.versionIDs[store.DecoderTag(dec.Version())]
	if !ok {
		return fmt.Errorf("decoder version %s has no decoder_version row", dec.Version())
	}
	pos := types.Position{Height: height, Time: time.Unix(0, tnano).UTC()}
	for _, m := range msgs {
		switch {
		case natsx.IsTxSubject(m.Subject):
			if err := h.flushTx(ctx, tx, dec, pos, decodedBy, m); err != nil {
				return err
			}
		case natsx.IsEventSubject(m.Subject):
			if err := h.flushEvent(ctx, tx, dec, pos, decodedBy, m); err != nil {
				return err
			}
		case natsx.IsKVSubject(m.Subject):
			if err := h.flushKV(ctx, tx, dec, pos, decodedBy, m); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected subject in supplier buffer: %s", m.Subject)
		}
	}
	return nil
}

func (h *Handler) flushTx(ctx context.Context, tx pgx.Tx, dec decoders.Decoder, pos types.Position, decodedBy int16, m consumer.Message) error {
	_, txIdx, err := natsx.HeightFromTxSubject(m.Subject)
	if err != nil {
		return err
	}
	var wrapped psv1.TxWithResult
	if err := wrapped.Unmarshal(m.Data); err != nil {
		return fmt.Errorf("TxWithResult %s: %w", m.Subject, err)
	}
	var result abci.ExecTxResult
	if len(wrapped.Result) > 0 {
		if err := result.Unmarshal(wrapped.Result); err != nil {
			return fmt.Errorf("ExecTxResult %s: %w", m.Subject, err)
		}
	}
	if result.Code != 0 {
		return nil // failed tx: no state change, no events, no KV (decision 7)
	}
	var cosmosTx sdktx.Tx
	if err := cosmosTx.Unmarshal(wrapped.Tx); err != nil {
		return fmt.Errorf("cosmos tx %s: %w", m.Subject, err)
	}
	for j, anyMsg := range cosmosTx.Body.Messages {
		decoded, err := dec.DecodeSupplierMsg(anyMsg.TypeUrl, anyMsg.Value)
		if err != nil {
			return err
		}
		if decoded == nil {
			continue
		}
		p := pos
		p.TxIndex, p.EventIndex = int32(txIdx), int32(j) // event_index column = msg index for msg tables
		switch {
		case decoded.Stake != nil:
			decoded.Stake.Position = p
			if err := store.InsertMsgStakeSupplier(ctx, tx, decoded.Stake, decodedBy); err != nil {
				return err
			}
		case decoded.Unstake != nil:
			decoded.Unstake.Position = p
			if err := store.InsertMsgUnstakeSupplier(ctx, tx, decoded.Unstake, decodedBy); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handler) flushEvent(ctx context.Context, tx pgx.Tx, dec decoders.Decoder, pos types.Position, decodedBy int16, m consumer.Message) error {
	var wrapped psv1.EventInBlock
	if err := wrapped.Unmarshal(m.Data); err != nil {
		return fmt.Errorf("EventInBlock %s: %w", m.Subject, err)
	}
	var ev abci.Event
	if err := ev.Unmarshal(wrapped.Event); err != nil {
		return fmt.Errorf("abci.Event %s: %w", m.Subject, err)
	}
	attrs := make([]types.EventAttr, 0, len(ev.Attributes))
	for _, a := range ev.Attributes {
		attrs = append(attrs, types.EventAttr{Key: a.Key, Value: a.Value})
	}
	decoded, err := dec.DecodeSupplierEvent(ev.Type, attrs)
	if err != nil {
		return err
	}
	if decoded == nil {
		return nil
	}
	p := pos
	p.TxIndex, p.EventIndex = max(wrapped.TxIndex, 0), wrapped.EventIndex // block-level (-1) stored as table-default 0
	switch {
	case decoded.Staked != nil:
		decoded.Staked.Position = p
		return store.InsertEventSupplierStaked(ctx, tx, decoded.Staked, decodedBy)
	case decoded.UnbondingBegin != nil:
		decoded.UnbondingBegin.Position = p
		return store.InsertEventSupplierUnbondingBegin(ctx, tx, decoded.UnbondingBegin, decodedBy)
	case decoded.UnbondingEnd != nil:
		decoded.UnbondingEnd.Position = p
		return store.InsertEventSupplierUnbondingEnd(ctx, tx, decoded.UnbondingEnd, decodedBy)
	case decoded.UnbondingCanceled != nil:
		decoded.UnbondingCanceled.Position = p
		return store.InsertEventSupplierUnbondingCanceled(ctx, tx, decoded.UnbondingCanceled, decodedBy)
	case decoded.ServiceConfigActivated != nil:
		decoded.ServiceConfigActivated.Position = p
		return store.InsertEventSupplierServiceConfigActivated(ctx, tx, decoded.ServiceConfigActivated, decodedBy)
	}
	return nil
}

func (h *Handler) flushKV(ctx context.Context, tx pgx.Tx, dec decoders.Decoder, pos types.Position, decodedBy int16, m consumer.Message) error {
	var kv storetypes.StoreKVPair
	if err := kv.Unmarshal(m.Data); err != nil {
		return fmt.Errorf("StoreKVPair %s: %w", m.Subject, err)
	}
	decoded, err := dec.DecodeSupplierKV(kv.Key, kv.Value, kv.Delete)
	if err != nil {
		return err
	}
	if decoded == nil {
		return nil
	}
	switch {
	case decoded.Supplier != nil:
		decoded.Supplier.Position = pos
		return store.InsertSupplierSnapshot(ctx, tx, decoded.Supplier, decodedBy)
	case decoded.ServiceConfigUpdate != nil:
		decoded.ServiceConfigUpdate.Position = pos
		return store.InsertServiceConfigUpdate(ctx, tx, decoded.ServiceConfigUpdate, decodedBy)
	}
	return nil
}
