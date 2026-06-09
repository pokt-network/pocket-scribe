package nats

import (
	"fmt"
	"strconv"
	"strings"
)

// StreamName is the JetStream stream that carries all PocketScribe chain
// messages. Single source of truth — do not redeclare elsewhere (DRY).
const StreamName = "POKT"

// StreamSubjects are the subject filters bound to StreamName.
var StreamSubjects = []string{"pokt.>"}

// BlockSubjectFilter is the wildcard a block-level consumer subscribes to (one
// message per height). Single source of truth — consumers and tests MUST use
// this constant rather than re-typing the literal (DRY).
const BlockSubjectFilter = "pokt.block.*"

const blockPrefix = "pokt.block."

// BlockSubject returns the subject carrying the block envelope for height h
// (ADR-022: one block-level message per height).
func BlockSubject(h int64) string {
	return blockPrefix + strconv.FormatInt(h, 10)
}

// HeightFromBlockSubject parses the height out of a pokt.block.<H> subject.
func HeightFromBlockSubject(subject string) (int64, error) {
	if !strings.HasPrefix(subject, blockPrefix) {
		return 0, fmt.Errorf("not a block subject: %q", subject)
	}
	rest := subject[len(blockPrefix):]
	if rest == "" || strings.Contains(rest, ".") {
		return 0, fmt.Errorf("malformed block subject: %q", subject)
	}
	h, err := strconv.ParseInt(rest, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height from %q: %w", subject, err)
	}
	return h, nil
}

// MsgID returns the deterministic Nats-Msg-Id for a message, derived from
// (subject, height, intra-block index) per ADR-022. Replaying the same logical
// message always yields the same id, enabling JetStream dedup.
func MsgID(subject string, height int64, index int) string {
	return fmt.Sprintf("%s|%d|%d", subject, height, index)
}
