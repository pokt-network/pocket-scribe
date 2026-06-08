#!/usr/bin/env bash
# Lightweight progress report for chapter 4 (v0.1.18..v0.1.33 sync).
# Designed to be invoked from cron / Claude — outputs ONE summary line.
set -u
cd "$(dirname "$0")/.."

NOW=$(date +%H:%M)
CURRENT_VER=$(ls -t run-v0.1.*.log 2>/dev/null | grep -v attempt | head -1 | sed 's|run-||;s|\.log||')
LAST_H=$(tail -200000 "run-${CURRENT_VER}.log" 2>/dev/null | sed -E 's/\x1b\[[0-9;]*m//g' | grep "committed state" | tail -1 | grep -oE 'height=[0-9]+' | cut -d= -f2)
TARGET=$(grep -A2 "^  - tag: $CURRENT_VER" versions.yaml | grep runs_until | grep -oE '[0-9]+|tip')
MIS=$(tail -200000 "run-${CURRENT_VER}.log" 2>/dev/null | sed -E 's/\x1b\[[0-9;]*m//g' | grep -c "wrong Block.Header")
PANIC=$(tail -200000 "run-${CURRENT_VER}.log" 2>/dev/null | sed -E 's/\x1b\[[0-9;]*m//g' | grep -c "^panic:")
SNAPS=$(ls snapshots/v0.1.{18,19,20,21,22,23,24,25,26,27,28,29,30,31,33}-h*-datadir.tar.xz 2>/dev/null | wc -l)
UPLOADED=$(rclone ls pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet/ 2>/dev/null | awk '{print $2}' | sed 's|/.*||' | sort -u | grep -E "^v0.1.(1[89]|2[0-9]|3[0-3])$" | wc -l)
NODE_ALIVE=$(pgrep -f "${CURRENT_VER}-archeology.*pocketd start" > /dev/null && echo yes || echo no)
ORCH_TAIL=$(tail -1 /tmp/orchestrator.log 2>/dev/null)
ORCH_ALIVE=$(pgrep -f "/tmp/orchestrator.sh" > /dev/null && echo yes || echo no)

echo "[$NOW] $CURRENT_VER h=$LAST_H/target=$TARGET mismatches=$MIS panics=$PANIC node=$NODE_ALIVE snaps=$SNAPS/15 uploaded=$UPLOADED/15"
echo "  orch=$ORCH_ALIVE: $ORCH_TAIL"
