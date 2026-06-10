package fileplugin

import (
	"fmt"
	"log/slog"

	"github.com/pokt-network/pocketscribe/internal/metrics"
)

const (
	// SoftCapBytes — payloads above this are logged (WARN) but still
	// published (spec §11.1 test 27).
	SoftCapBytes = 256 << 10
	// HardCapBytes — payloads above this are REFUSED: the NATS server's
	// default max_payload is 1 MiB, so the publish would fail server-side
	// anyway; refusing at the source keeps the failure explicit and the
	// height un-acked (no silent message loss).
	HardCapBytes = 1 << 20
)

type publishFn func(subj string, data []byte, msgID string) error

// capPublish decorates publish with the payload size policy. The returned
// error on a hard-cap violation aborts the whole height (Bootstrap stops at
// that height) — the sidecar must never skip a message inside a block.
// fpm may be nil (tests without a registry).
func capPublish(publish publishFn, logger *slog.Logger, fpm *metrics.FilePlugin) publishFn {
	return func(subj string, data []byte, msgID string) error {
		switch {
		case len(data) > HardCapBytes:
			if fpm != nil {
				fpm.OversizeRefused.Inc()
			}
			logger.Error("payload exceeds 1 MiB hard cap; refusing to publish",
				"subject", subj, "bytes", len(data), "msg_id", msgID)
			return fmt.Errorf("publish %s: payload %d bytes exceeds %d-byte hard cap", subj, len(data), HardCapBytes)
		case len(data) > SoftCapBytes:
			if fpm != nil {
				fpm.OversizeSoft.Inc()
			}
			logger.Warn("payload exceeds 256 KiB soft cap",
				"subject", subj, "bytes", len(data), "msg_id", msgID)
		}
		return publish(subj, data, msgID)
	}
}
