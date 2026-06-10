package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// nullStr converts an empty string to nil (SQL NULL) so that nullable TEXT
// columns receive NULL rather than an empty string, consistent with the
// append-only schema convention (empty is distinguishable from absent).
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// InsertMsgStakeSupplier writes one row to msg_stake_supplier.
// Idempotent: ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING.
func InsertMsgStakeSupplier(ctx context.Context, tx pgx.Tx, r *types.MsgStakeSupplier, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO msg_stake_supplier
		 (signer, owner_address, operator_address, stake_amount, stake_denom, services,
		  block_height, block_time, tx_index, event_index, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING`,
		nullStr(r.Signer), nullStr(r.OwnerAddress), nullStr(r.OperatorAddress),
		r.StakeAmount, nullStr(r.StakeDenom), r.ServicesJSON,
		r.Height, r.Time, r.TxIndex, r.EventIndex, decodedBy)
	if err != nil {
		return fmt.Errorf("insert msg_stake_supplier at height %d: %w", r.Height, err)
	}
	return nil
}

// InsertMsgUnstakeSupplier writes one row to msg_unstake_supplier.
// Idempotent: ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING.
func InsertMsgUnstakeSupplier(ctx context.Context, tx pgx.Tx, r *types.MsgUnstakeSupplier, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO msg_unstake_supplier
		 (signer, operator_address, block_height, block_time, tx_index, event_index, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING`,
		nullStr(r.Signer), nullStr(r.OperatorAddress),
		r.Height, r.Time, r.TxIndex, r.EventIndex, decodedBy)
	if err != nil {
		return fmt.Errorf("insert msg_unstake_supplier at height %d: %w", r.Height, err)
	}
	return nil
}

// InsertEventSupplierStaked writes one row to event_supplier_staked.
// Idempotent: ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING.
func InsertEventSupplierStaked(ctx context.Context, tx pgx.Tx, r *types.EventSupplierStaked, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO event_supplier_staked
		 (supplier, session_end_height, operator_address, block_height, block_time, tx_index, event_index, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING`,
		r.SupplierJSON, r.SessionEndHeight, nullStr(r.OperatorAddress),
		r.Height, r.Time, r.TxIndex, r.EventIndex, decodedBy)
	if err != nil {
		return fmt.Errorf("insert event_supplier_staked at height %d: %w", r.Height, err)
	}
	return nil
}

// InsertEventSupplierUnbondingBegin writes one row to event_supplier_unbonding_begin.
// Idempotent: ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING.
func InsertEventSupplierUnbondingBegin(ctx context.Context, tx pgx.Tx, r *types.EventSupplierUnbondingBegin, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO event_supplier_unbonding_begin
		 (supplier, reason, session_end_height, unbonding_end_height, block_height, block_time, tx_index, event_index, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING`,
		r.SupplierJSON, r.ReasonJSON, r.SessionEndHeight, r.UnbondingEndHeight,
		r.Height, r.Time, r.TxIndex, r.EventIndex, decodedBy)
	if err != nil {
		return fmt.Errorf("insert event_supplier_unbonding_begin at height %d: %w", r.Height, err)
	}
	return nil
}

// InsertEventSupplierUnbondingEnd writes one row to event_supplier_unbonding_end.
// Idempotent: ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING.
func InsertEventSupplierUnbondingEnd(ctx context.Context, tx pgx.Tx, r *types.EventSupplierUnbondingEnd, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO event_supplier_unbonding_end
		 (supplier, reason, session_end_height, unbonding_end_height, block_height, block_time, tx_index, event_index, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING`,
		r.SupplierJSON, r.ReasonJSON, r.SessionEndHeight, r.UnbondingEndHeight,
		r.Height, r.Time, r.TxIndex, r.EventIndex, decodedBy)
	if err != nil {
		return fmt.Errorf("insert event_supplier_unbonding_end at height %d: %w", r.Height, err)
	}
	return nil
}

// InsertEventSupplierUnbondingCanceled writes one row to event_supplier_unbonding_canceled.
// Idempotent: ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING.
func InsertEventSupplierUnbondingCanceled(ctx context.Context, tx pgx.Tx, r *types.EventSupplierUnbondingCanceled, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO event_supplier_unbonding_canceled
		 (supplier, height, session_end_height, block_height, block_time, tx_index, event_index, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING`,
		r.SupplierJSON, r.AtHeight, r.SessionEndHeight,
		r.Height, r.Time, r.TxIndex, r.EventIndex, decodedBy)
	if err != nil {
		return fmt.Errorf("insert event_supplier_unbonding_canceled at height %d: %w", r.Height, err)
	}
	return nil
}

// InsertEventSupplierServiceConfigActivated writes one row to
// event_supplier_service_config_activated.
// Idempotent: ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING.
func InsertEventSupplierServiceConfigActivated(ctx context.Context, tx pgx.Tx, r *types.EventSupplierServiceConfigActivated, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO event_supplier_service_config_activated
		 (supplier, activation_height, operator_address, service_id, block_height, block_time, tx_index, event_index, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING`,
		r.SupplierJSON, r.ActivationHeight, nullStr(r.OperatorAddress), nullStr(r.ServiceID),
		r.Height, r.Time, r.TxIndex, r.EventIndex, decodedBy)
	if err != nil {
		return fmt.Errorf("insert event_supplier_service_config_activated at height %d: %w", r.Height, err)
	}
	return nil
}

// InsertSupplierSnapshot writes one row to supplier_history.
// Idempotent: ON CONFLICT (operator_address, block_height) DO NOTHING.
func InsertSupplierSnapshot(ctx context.Context, tx pgx.Tx, r *types.SupplierSnapshot, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO supplier_history
		 (operator_address, owner_address, stake_amount, stake_denom, services,
		  unstake_session_end_height, service_config_history, block_height, block_time, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		 ON CONFLICT (operator_address, block_height) DO NOTHING`,
		nullStr(r.OperatorAddress), nullStr(r.OwnerAddress),
		r.StakeAmount, nullStr(r.StakeDenom), r.ServicesJSON,
		r.UnstakeSessionEndHeight, r.ServiceConfigHistoryJSON,
		r.Height, r.Time, decodedBy)
	if err != nil {
		return fmt.Errorf("insert supplier_history at height %d: %w", r.Height, err)
	}
	return nil
}

// InsertServiceConfigUpdate writes one row to supplier_service_config_update_history.
// DO UPDATE fires ONLY when the SAME (operator, service_id, activation_height) KV key
// is written more than once at the SAME block_height (e.g. update + deletion in one
// block). It is NEVER a cross-height update — block_height is in the PK (append-only
// preserved). Deterministic KV enumeration order makes the same-height last-write-wins
// idempotent across replays.
func InsertServiceConfigUpdate(ctx context.Context, tx pgx.Tx, r *types.ServiceConfigUpdateSnapshot, decodedBy int16) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO supplier_service_config_update_history
		 (operator_address, service_id, activation_height, deactivation_height,
		  service_config, deleted, block_height, block_time, decoded_by_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (operator_address, service_id, activation_height, block_height) DO UPDATE
		 SET deactivation_height = EXCLUDED.deactivation_height,
		     service_config      = EXCLUDED.service_config,
		     deleted             = EXCLUDED.deleted,
		     decoded_by_version  = EXCLUDED.decoded_by_version`,
		nullStr(r.OperatorAddress), nullStr(r.ServiceID),
		r.ActivationHeight, r.DeactivationHeight,
		r.ServiceConfigJSON, r.Deleted,
		r.Height, r.Time, decodedBy)
	if err != nil {
		return fmt.Errorf("insert supplier_service_config_update_history at height %d: %w", r.Height, err)
	}
	return nil
}
