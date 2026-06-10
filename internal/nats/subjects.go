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

// ── tx fan-out (ADR-022: pokt.tx.{H}.{idx}, one tx per message) ─────────────

// TxSubjectFilter matches every per-tx message regardless of height/index.
const TxSubjectFilter = "pokt.tx.>"

const txPrefix = "pokt.tx."

// TxSubject returns the subject for tx index idx of height h.
func TxSubject(h int64, idx int) string {
	return txPrefix + strconv.FormatInt(h, 10) + "." + strconv.Itoa(idx)
}

// HeightFromTxSubject parses pokt.tx.{H}.{idx}.
func HeightFromTxSubject(subject string) (int64, int, error) {
	if !strings.HasPrefix(subject, txPrefix) {
		return 0, 0, fmt.Errorf("not a tx subject: %q", subject)
	}
	rest := strings.Split(subject[len(txPrefix):], ".")
	if len(rest) != 2 {
		return 0, 0, fmt.Errorf("malformed tx subject: %q", subject)
	}
	h, err := strconv.ParseInt(rest[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse height from %q: %w", subject, err)
	}
	idx, err := strconv.Atoi(rest[1])
	if err != nil {
		return 0, 0, fmt.Errorf("parse tx index from %q: %w", subject, err)
	}
	return h, idx, nil
}

// ── event fan-out (ADR-022: pokt.events.{eventType}.{H}) ────────────────────

const eventPrefix = "pokt.events."

// EventToken converts an ABCI event type to a single NATS token: "." is the
// NATS separator, so "pocket.supplier.EventSupplierStaked" becomes
// "pocket_supplier_EventSupplierStaked" (ADR-022 amendment).
func EventToken(eventType string) string { return strings.ReplaceAll(eventType, ".", "_") }

// EventSubject returns the subject for one event of eventType at height h.
func EventSubject(eventType string, h int64) string {
	return eventPrefix + EventToken(eventType) + "." + strconv.FormatInt(h, 10)
}

// EventSubjectFilter matches all heights of one event type.
func EventSubjectFilter(eventType string) string { return eventPrefix + EventToken(eventType) + ".*" }

// HeightFromEventSubject parses pokt.events.{token}.{H}.
func HeightFromEventSubject(subject string) (int64, error) {
	if !strings.HasPrefix(subject, eventPrefix) {
		return 0, fmt.Errorf("not an event subject: %q", subject)
	}
	rest := strings.Split(subject[len(eventPrefix):], ".")
	if len(rest) != 2 {
		return 0, fmt.Errorf("malformed event subject: %q", subject)
	}
	h, err := strconv.ParseInt(rest[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height from %q: %w", subject, err)
	}
	return h, nil
}

// ── kv fan-out (ADR-022: pokt.kv.{store}.{H}) ───────────────────────────────

const kvPrefix = "pokt.kv."

// KVSubject returns the subject for one StoreKVPair of store at height h.
func KVSubject(store string, h int64) string {
	return kvPrefix + store + "." + strconv.FormatInt(h, 10)
}

// KVSubjectFilter matches all heights of one store.
func KVSubjectFilter(store string) string { return kvPrefix + store + ".*" }

// HeightFromKVSubject parses pokt.kv.{store}.{H}.
func HeightFromKVSubject(subject string) (int64, error) {
	if !strings.HasPrefix(subject, kvPrefix) {
		return 0, fmt.Errorf("not a kv subject: %q", subject)
	}
	rest := strings.Split(subject[len(kvPrefix):], ".")
	if len(rest) != 2 {
		return 0, fmt.Errorf("malformed kv subject: %q", subject)
	}
	h, err := strconv.ParseInt(rest[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height from %q: %w", subject, err)
	}
	return h, nil
}

// ── subject classification helpers (rule 7: single source of truth) ─────────

// IsBlockSubject reports whether s is a pokt.block.{H} subject.
func IsBlockSubject(s string) bool { return strings.HasPrefix(s, blockPrefix) }

// IsTxSubject reports whether s is a pokt.tx.{H}.{idx} subject.
func IsTxSubject(s string) bool { return strings.HasPrefix(s, txPrefix) }

// IsEventSubject reports whether s is a pokt.events.{token}.{H} subject.
func IsEventSubject(s string) bool { return strings.HasPrefix(s, eventPrefix) }

// IsKVSubject reports whether s is a pokt.kv.{store}.{H} subject.
func IsKVSubject(s string) bool { return strings.HasPrefix(s, kvPrefix) }

// HeightFromSubject extracts the height from any PocketScribe subject grammar
// (block / tx / events / kv). Single dispatch point for the consumer runtimes.
func HeightFromSubject(subject string) (int64, error) {
	switch {
	case strings.HasPrefix(subject, blockPrefix):
		return HeightFromBlockSubject(subject)
	case strings.HasPrefix(subject, txPrefix):
		h, _, err := HeightFromTxSubject(subject)
		return h, err
	case strings.HasPrefix(subject, eventPrefix):
		return HeightFromEventSubject(subject)
	case strings.HasPrefix(subject, kvPrefix):
		return HeightFromKVSubject(subject)
	default:
		return 0, fmt.Errorf("unknown subject grammar: %q", subject)
	}
}
