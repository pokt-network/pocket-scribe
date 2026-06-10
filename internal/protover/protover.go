// Package protover parses and compares poktroll protocol versions. It is the
// SINGLE boundary where version strings are normalized (spec §4.10: semver
// comparison, never lexicographic; internal code never compares raw strings).
// Two spellings exist in the system — dotted ("v0.1.30": upgrades.name,
// consumer_registry.first_valid_version) and underscored decoder-dir form
// ("v0_1_30": network.genesis_decoder_version, upgrades.decoder_version) —
// Normalize accepts both and returns the canonical dotted form.
package protover

import (
	"fmt"
	"strings"

	"golang.org/x/mod/semver"
)

// Normalize returns the canonical dotted form ("vMAJOR.MINOR.PATCH") of a
// version given in dotted or underscored spelling. Errors on anything that is
// not a valid semver tag with a leading "v".
func Normalize(s string) (string, error) {
	c := strings.ReplaceAll(strings.TrimSpace(s), "_", ".")
	if !semver.IsValid(c) {
		return "", fmt.Errorf("invalid protocol version %q", s)
	}
	return semver.Canonical(c), nil
}

// Compare orders two canonical versions: -1 if a < b, 0 if equal, +1 if a > b.
// Inputs MUST come from Normalize (garbage compares as lowest in x/mod/semver,
// silently — hence the boundary discipline).
func Compare(a, b string) int { return semver.Compare(a, b) }

// ToDecoderDir converts any accepted spelling to the decoder directory
// spelling ("v0.1.30" → "v0_1_30").
func ToDecoderDir(s string) (string, error) {
	n, err := Normalize(s)
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(n, ".", "_"), nil
}
