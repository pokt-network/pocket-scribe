// Package types provides archeological stub types for Phase A skeleton.
//
// This file is a stub to allow morse_claimable_account_shim.go to compile during the skeleton phase.
// When proto files are available, this will be replaced with actual generated code.
package types

import "time"

// MorseClaimableAccount is a placeholder type during Phase A skeleton.
type MorseClaimableAccount struct {
	ShannonDestAddress string
	ClaimedAtHeight    int64
	UnstakingTime      time.Time
	MorseSrcAddress    string
}

func (m *MorseClaimableAccount) GetMorseSrcAddress() string {
	if m == nil {
		return ""
	}
	return m.MorseSrcAddress
}
