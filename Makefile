# PocketScribe — Makefile
#
# Targets exposed here are TESTED to work today. As we add real Go code,
# build/test/lint targets will grow. The current repo state is skills +
# schema migrations + archeology artifacts; the targets below reflect that.

.PHONY: help \
        verify-migrations regenerate-migrations regenerate-snapshots \
        clean

help: ## Print this help
	@awk 'BEGIN {FS = ":.*?## "} \
	     /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ─── Schema migrations ─────────────────────────────────────────────────────

verify-migrations: ## Apply schema/migrations/* against a fresh TimescaleDB (docker)
	@bash .claude/skills/verify-migrations/run.sh

regenerate-snapshots: ## Re-extract proto shape snapshots for every vendored version
	@for vdir in third_party/proto/poktroll/*/; do \
	  v=$$(basename $$vdir | tr '_' '.'); \
	  echo "=== $$v ==="; \
	  bash .claude/skills/generate-decoder/run.sh $$v >/dev/null; \
	done
	@echo "Snapshots in docs/research/.shapes/"

regenerate-migrations: ## Re-generate schema/migrations/00NN_decoder_*.sql from snapshots
	@rm -f schema/migrations/00*_decoder_v0_1_*.sql
	@for vdir in third_party/proto/poktroll/*/; do \
	  v=$$(basename $$vdir | tr '_' '.'); \
	  bash .claude/skills/generate-migration-from-diff/run.sh $$v >/dev/null; \
	done
	@ls schema/migrations/ | grep _decoder_ | wc -l | awk '{print "Migrations: "$$1}'

# ─── Housekeeping ──────────────────────────────────────────────────────────

clean: ## Remove transient outputs (.shapes, generated migrations)
	@rm -rf docs/research/.shapes/
	@rm -f schema/migrations/00*_decoder_v0_1_*.sql
	@echo "Cleaned snapshots + decoder migrations."
