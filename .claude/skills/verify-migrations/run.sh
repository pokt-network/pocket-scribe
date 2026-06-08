#!/usr/bin/env bash
# /verify-migrations — apply all goose migrations against a disposable
# TimescaleDB container. See SKILL.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROOT"

DIR="schema/migrations"
KEEP_DB=0
TARGET=""

while [ $# -gt 0 ]; do
  case "$1" in
    --dir) DIR="$2"; shift 2 ;;
    --keep-db) KEEP_DB=1; shift ;;
    --target) TARGET="$2"; shift 2 ;;
    -h|--help) sed -n '1,40p' "$SCRIPT_DIR/SKILL.md"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [ ! -d "$DIR" ]; then
  echo "FAIL: migrations dir not found: $DIR" >&2
  exit 3
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "FAIL: docker not installed." >&2
  exit 4
fi

CONTAINER="ps-verify-db"
PORT=15432
PWD="verify"
DSN="host=localhost port=$PORT user=postgres password=$PWD dbname=postgres sslmode=disable"

# Step 1: bring up container if not running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[1/5] starting $CONTAINER..."
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker run -d \
      --name "$CONTAINER" \
      -e POSTGRES_PASSWORD="$PWD" \
      -p $PORT:5432 \
      timescale/timescaledb:latest-pg18 \
      >/dev/null
else
  echo "[1/5] reusing running $CONTAINER..."
fi

# Step 2: wait for ready
echo "[2/5] waiting for DB ready..."
for i in $(seq 1 45); do
  if docker exec "$CONTAINER" pg_isready -U postgres -q 2>/dev/null; then
    break
  fi
  sleep 1
done
if ! docker exec "$CONTAINER" pg_isready -U postgres -q 2>/dev/null; then
  echo "FAIL: DB never became ready." >&2
  exit 5
fi

# Step 3: reset schema
echo "[3/5] resetting public schema..."
docker exec "$CONTAINER" psql -U postgres -q -c \
    "DROP SCHEMA IF EXISTS public CASCADE; \
     DROP SCHEMA IF EXISTS _timescaledb_internal CASCADE; \
     CREATE SCHEMA public; \
     DROP TABLE IF EXISTS goose_db_version;" \
    >/dev/null

# Step 4: run goose up
echo "[4/5] applying migrations from $DIR..."
LOG="/tmp/verify-migrations-$$.log"
GOOSE_CMD="go run github.com/pressly/goose/v3/cmd/goose@latest -dir $DIR postgres \"$DSN\""
if [ -n "$TARGET" ]; then
  GOOSE_CMD="$GOOSE_CMD up-to $TARGET"
else
  GOOSE_CMD="$GOOSE_CMD up"
fi
if eval "$GOOSE_CMD" >"$LOG" 2>&1; then
  RESULT=0
else
  RESULT=1
fi

# Step 5: report
echo
if [ $RESULT -eq 0 ]; then
  TABLES=$(docker exec "$CONTAINER" psql -U postgres -tAc \
      "SELECT count(*) FROM pg_tables WHERE schemaname='public';" 2>/dev/null || echo "?")
  HYPER=$(docker exec "$CONTAINER" psql -U postgres -tAc \
      "SELECT count(*) FROM timescaledb_information.hypertables;" 2>/dev/null || echo "0")
  SIZE=$(docker exec "$CONTAINER" psql -U postgres -tAc \
      "SELECT pg_size_pretty(pg_database_size('postgres'));" 2>/dev/null || echo "?")
  echo "✅ All migrations applied OK."
  echo "   Tables: $TABLES"
  echo "   Hypertables: $HYPER"
  echo "   Schema size: $SIZE"
else
  echo "❌ Migration verification FAILED. Excerpt:"
  echo
  # Find the failing migration and PG error
  FAILED=$(grep -oE "ERROR [0-9_a-z]+\.sql" "$LOG" | head -1 | awk '{print $2}')
  PG_ERR=$(grep -oE "ERROR:[^\"]+SQLSTATE \w+" "$LOG" | head -1 || \
           grep -oE "ERROR:[^\"]+" "$LOG" | head -1)
  echo "   Migration:   ${FAILED:-?}"
  echo "   PG error:    ${PG_ERR:-?}"
  echo
  echo "   Full log: $LOG"
fi

# Cleanup
if [ $KEEP_DB -eq 0 ]; then
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
else
  echo
  echo "   Container kept running. DSN:"
  echo "   $DSN"
fi

exit $RESULT
