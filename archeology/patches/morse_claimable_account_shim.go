//go:build archeology
// +build archeology

package types

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/pokt-network/poktroll/app/pocket"
)

// IsClaimed returns true if the MorseClaimableAccount has been claimed;
// i.e. ShannonDestAddress is not empty OR the ClaimedAtHeight is greater than 0.
func (m *MorseClaimableAccount) IsClaimed() bool {
	return m.ShannonDestAddress != "" || m.ClaimedAtHeight > 0
}

// IsUnbonding indicates that the MorseClaimableAccount began unbonding on Morse
// but its unbonding period has NOT yet elapsed at the time that the Morse snapshot was taken.
func (m *MorseClaimableAccount) IsUnbonding() bool {
	// DEV_NOTE: The UnstakingTime field is a time.Time type, which has a zero value of "0001-01-01T00:00:00Z" when printed as an ISO8601 string.
	// See: https://pkg.go.dev/time#Time.IsZero
	return !m.UnstakingTime.IsZero()
}

// HasUnbonded indicates that the MorseClaimableAccount began unbonding on Morse
// and the unbonding period has elapsed. E.g., the supplier was claimed > 21 days
// after it began unbonding.
//
// ARCHEOLOGY PATCH: takes ctx and uses BlockTime() instead of wall-clock
// time.Until. Equivalent to poktroll PR #1436 (v0.1.17 fix) applied
// retroactively to v0.1.15/v0.1.16 for deterministic replay.
func (m *MorseClaimableAccount) HasUnbonded(ctx context.Context) bool {
	return m.IsUnbonding() && m.SecondsUntilUnbonded(ctx) <= 0
}

// SecondsUntilUnbonded returns the number of seconds until the MorseClaimableAccount's
// unbonding period will elapse.
//
// ARCHEOLOGY PATCH: takes ctx and computes via BlockTime() instead of wall-clock.
func (m *MorseClaimableAccount) SecondsUntilUnbonded(ctx context.Context) int64 {
	sdkCtx := cosmostypes.UnwrapSDKContext(ctx)
	durationUntilUnbonded := m.UnstakingTime.Sub(sdkCtx.BlockTime())
	return int64(durationUntilUnbonded.Seconds())
}

// canonicalUnbondingEndHeightOverrides hardcodes the canonical (chain-accepted)
// unstake_session_end_height values for the small set of MsgClaimMorseSupplier
// / MsgClaimMorseApplication transactions in the v0.1.16 range
// (heights 99293..102141) that exercised the non-deterministic time.Until
// codepath. The canonical chain (running v0.1.16 release binary) computed
// these via wall-clock time at finalization, which cannot be reproduced
// from BlockTime alone. To preserve byte-perfect AppHash during replay we
// short-circuit GetEstimatedUnbondingEndHeight with the canonical value
// extracted from chain events.
//
// Key format: "<MORSE_ADDR_UPPERCASE>|<BLOCK_HEIGHT>".
// Values sourced from pocket.migration.EventMorseSupplierClaimed
// supplier.unstake_session_end_height attribute on mainnet.
var canonicalUnbondingEndHeightOverrides = map[string]int64{
	"BE6559ECD278F68D375F6215198383760E49BD43|99295": 106578,
	"8419C72D166D5FD411BCF38D3C9FC1C7ED2AFF8E|99345": 106603,
}

// GetEstimatedUnbondingEndHeight returns the estimated block height at which the
// MorseClaimableAccount's unbonding period will end.
//
// ARCHEOLOGY PATCH: first consults a hardcoded canonical override table for
// v0.1.16-specific non-deterministic computations; falls back to a deterministic
// BlockTime-based estimate (equivalent to the time portion of poktroll PR #1436).
// We deliberately do NOT apply PR #1436's GetSessionEndHeight wrapping, because
// the v0.1.15/v0.1.16 canonical chain committed the raw estimate.
func (m *MorseClaimableAccount) GetEstimatedUnbondingEndHeight(ctx context.Context) (height int64, isUnbonded bool) {
	sdkCtx := cosmostypes.UnwrapSDKContext(ctx)

	overrideKey := fmt.Sprintf("%s|%d", strings.ToUpper(m.GetMorseSrcAddress()), sdkCtx.BlockHeight())
	if h, ok := canonicalUnbondingEndHeightOverrides[overrideKey]; ok {
		return h, false
	}

	// Retrieve the estimated block duration for the current chain from a lookup table.
	estimatedBlockDuration, ok := pocket.EstimatedBlockDurationByChainId[sdkCtx.ChainID()]
	if !ok || estimatedBlockDuration == 0 {
		return -1, false
	}

	// Check if unstaking is complete via deterministic BlockTime (not wall clock).
	durationUntilUnstakeCompletion := m.UnstakingTime.Sub(sdkCtx.BlockTime())
	if durationUntilUnstakeCompletion <= 0 {
		return -1, true
	}

	// Calculate the estimated Shannon unstake session end height.
	estimatedBlocksUntilUnstakeCompletion := big.NewRat(int64(durationUntilUnstakeCompletion), int64(estimatedBlockDuration))
	estimatedUnstakeCompletionHeight := new(big.Rat).Add(
		big.NewRat(sdkCtx.BlockHeight(), 1),
		estimatedBlocksUntilUnstakeCompletion,
	)
	return new(big.Int).Div(
		estimatedUnstakeCompletionHeight.Num(),
		estimatedUnstakeCompletionHeight.Denom(),
	).Int64(), false
}
