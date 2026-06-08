#!/usr/bin/env bash
# Orchestrator: run a sequence of versions, snapshotting between each.
#
# Usage:
#   scripts/run-chapter.sh <from-version> <to-version>
#     Run from <from> through <to> inclusive, stopping at each version's
#     `runs_until` height. Produces one tarball pair per version.
#
#   scripts/run-chapter.sh --chapter <N>
#     Run all versions whose upload_chapter == N (from versions.yaml).
#
#   scripts/run-chapter.sh --resume
#     Continue from .state/last_completed_version.
#
# Idempotency: each version's snapshot is written only once. Re-runs detect
# existing snapshots and skip them.
#
# Halting: SIGINT here cascades cleanly to the running node (no orphaned
# pids; current version's snapshot is NOT taken if interrupted mid-sync).
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh
load_env

usage() {
  sed -n '1,/^set -euo pipefail$/p' "$0" | grep '^# \?' | sed 's/^# \?//'
  exit "${1:-1}"
}

# ─── Parse args ──────────────────────────────────────────────────────────
FROM=""
TO=""
CHAPTER=""
RESUME=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --chapter)  CHAPTER="$2"; shift 2 ;;
    --resume)   RESUME=true; shift ;;
    -h|--help)  usage 0 ;;
    *)
      if [[ -z "$FROM" ]]; then FROM="$1"
      elif [[ -z "$TO" ]]; then TO="$1"
      else die "unexpected argument: $1"
      fi
      shift
      ;;
  esac
done

# ─── Build the run list ──────────────────────────────────────────────────
ALL_VERSIONS=($(list_versions))

if [[ "$RESUME" == "true" ]]; then
  LAST="$(state_get last_completed_version)"
  if [[ -z "$LAST" ]]; then
    log "no prior progress recorded; starting from the first version"
    FROM="${ALL_VERSIONS[0]}"
  else
    # Start from the version AFTER last completed
    found=false
    for i in "${!ALL_VERSIONS[@]}"; do
      if [[ "${ALL_VERSIONS[i]}" == "$LAST" ]]; then
        next=$((i + 1))
        if (( next >= ${#ALL_VERSIONS[@]} )); then
          ok "all versions already completed (last: $LAST). Nothing to do."
          exit 0
        fi
        FROM="${ALL_VERSIONS[next]}"
        found=true
        break
      fi
    done
    [[ "$found" == "true" ]] || die "last_completed_version=$LAST not found in versions.yaml — manual fix needed"
  fi
  TO="${ALL_VERSIONS[-1]}"
fi

if [[ -n "$CHAPTER" ]]; then
  # Filter ALL_VERSIONS to those whose upload_chapter == CHAPTER
  RUN_LIST=()
  for v in "${ALL_VERSIONS[@]}"; do
    if [[ "$(version_field "$v" upload_chapter)" == "$CHAPTER" ]]; then
      RUN_LIST+=("$v")
    fi
  done
  if (( ${#RUN_LIST[@]} == 0 )); then
    die "no versions found with upload_chapter=$CHAPTER"
  fi
else
  [[ -n "$FROM" && -n "$TO" ]] || usage 1
  # Slice the ordered list between FROM and TO inclusive
  RUN_LIST=()
  in_range=false
  for v in "${ALL_VERSIONS[@]}"; do
    if [[ "$v" == "$FROM" ]]; then in_range=true; fi
    if [[ "$in_range" == "true" ]]; then RUN_LIST+=("$v"); fi
    if [[ "$v" == "$TO" ]]; then in_range=false; fi
  done
  (( ${#RUN_LIST[@]} > 0 )) || die "no versions in range $FROM..$TO"
fi

log "running ${#RUN_LIST[@]} versions: ${RUN_LIST[*]}"

# ─── Execute one version at a time ───────────────────────────────────────
for VERSION in "${RUN_LIST[@]}"; do
  RUNS_UNTIL="$(version_field "$VERSION" runs_until)"
  if [[ "$RUNS_UNTIL" == "tip" ]]; then
    TIP="$(sauron_tip)"
    log "version $VERSION runs until tip — using current Sauron tip $TIP"
    RUNS_UNTIL="$TIP"
  fi

  SNAP_NAME="${VERSION}-h${RUNS_UNTIL}"
  if [[ -f "$SNAPSHOTS_DIR/${SNAP_NAME}-datadir.tar.xz" ]]; then
    log "snapshot ${SNAP_NAME} already exists — skipping"
    continue
  fi

  log "=============================================================="
  log " CHAPTER: $VERSION → height $RUNS_UNTIL"
  log "=============================================================="

  scripts/30-run-version.sh "$VERSION" "$RUNS_UNTIL"
  REACHED="$(state_get last_completed_height)"
  scripts/40-snapshot-version.sh "$VERSION" "$REACHED"

  ok "chapter $VERSION → $REACHED complete"
done

ok "all requested versions complete"
