#!/usr/bin/env python3
"""Diff a snapshot against the immediately-previous one (by semver).

Usage: diff.py <snapshot.json>

Finds the largest snapshot under <snapshot.json>'s directory whose version is
strictly less than the input's, parses both, emits a JSON diff on stdout.
"""

import json
import re
import sys
from pathlib import Path

VERSION_RE = re.compile(r"v(\d+)_(\d+)_(\d+)")


def semver_key(stem: str) -> tuple[int, int, int]:
    m = VERSION_RE.fullmatch(stem)
    if not m:
        return (0, 0, 0)
    return tuple(int(x) for x in m.groups())  # type: ignore[return-value]


def field_eq(a: dict, b: dict) -> bool:
    return a["type"] == b["type"] and a["repeated"] == b["repeated"]


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: diff.py <snapshot.json>", file=sys.stderr)
        return 2
    cur_path = Path(sys.argv[1])
    cur = json.loads(cur_path.read_text())
    cur_key = semver_key(cur_path.stem)
    candidates = [
        p
        for p in cur_path.parent.glob("v*.json")
        if p != cur_path and semver_key(p.stem) < cur_key
    ]
    if not candidates:
        out = {
            "version": cur["version"],
            "previous_version": None,
            "added_messages": sorted(cur["messages"].keys()),
            "removed_messages": [],
            "changed_messages": {},
            "unchanged_messages_count": 0,
        }
        json.dump(out, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 0

    prev_path = max(candidates, key=lambda p: semver_key(p.stem))
    prev = json.loads(prev_path.read_text())

    cur_msgs = cur["messages"]
    prev_msgs = prev["messages"]
    added = sorted(set(cur_msgs) - set(prev_msgs))
    removed = sorted(set(prev_msgs) - set(cur_msgs))
    common = sorted(set(cur_msgs) & set(prev_msgs))

    changed: dict[str, dict] = {}
    unchanged = 0
    for name in common:
        c_fields = {f["name"]: f for f in cur_msgs[name]["fields"]}
        p_fields = {f["name"]: f for f in prev_msgs[name]["fields"]}
        added_f = [c_fields[n] for n in sorted(c_fields) if n not in p_fields]
        removed_f = [p_fields[n] for n in sorted(p_fields) if n not in c_fields]
        type_changed_f = []
        for n in sorted(set(c_fields) & set(p_fields)):
            if not field_eq(c_fields[n], p_fields[n]):
                type_changed_f.append(
                    {"name": n, "old": p_fields[n], "new": c_fields[n]}
                )
        if added_f or removed_f or type_changed_f:
            changed[name] = {
                "added_fields": added_f,
                "removed_fields": removed_f,
                "type_changed_fields": type_changed_f,
            }
        else:
            unchanged += 1

    out = {
        "version": cur["version"],
        "previous_version": prev["version"],
        "added_messages": added,
        "removed_messages": removed,
        "changed_messages": changed,
        "unchanged_messages_count": unchanged,
    }
    json.dump(out, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
