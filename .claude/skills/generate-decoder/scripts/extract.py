#!/usr/bin/env python3
"""Extract poktroll-owned message shapes from a directory of vendored .proto files.

Usage: extract.py <vendored_proto_dir>

Reads every *.proto file under <dir>/pocket/ (poktroll-owned packages), parses
each top-level `message` block, and emits a JSON document on stdout with the
shape of every message found.

Environment:
- VERSION_TAG  — written into output (e.g. "v0.1.5"). Defaults to dir basename.
- VENDORED_AT  — ISO timestamp written into output. Defaults to "unknown".

Output shape (keys sorted):
{
  "version": "v0.1.5",
  "vendored_at": "...",
  "messages": {
    "pocket.shared.Supplier": {
      "file": "pocket/shared/supplier.proto",
      "fields": [
        {"name": "owner_address", "type": "string", "tag": 1, "repeated": false,
         "comment": "Owner address that controls the staked funds..."},
        ...
      ]
    },
    ...
  }
}
"""

import json
import os
import re
import sys
from pathlib import Path

FIELD_RE = re.compile(
    r"""
    ^\s*
    (?:(?P<rule>repeated|optional|required)\s+)?
    (?P<type>[\w.]+)
    \s+
    (?P<name>\w+)
    \s*=\s*
    (?P<tag>\d+)
    (?:\s*\[[^\]]*\])?
    \s*;
    """,
    re.VERBOSE,
)

PACKAGE_RE = re.compile(r"^\s*package\s+([\w.]+)\s*;", re.MULTILINE)


def strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    text = re.sub(r"//[^\n]*", "", text)
    return text


def find_top_level_messages(text: str):
    """Yield (name, body_text) for each top-level `message X { ... }` block.

    Body excludes the wrapping braces. Nested messages remain in body and are
    handled by parse_fields (skipped).
    """
    pos = 0
    while True:
        m = re.search(r"\bmessage\s+(\w+)\s*\{", text[pos:])
        if not m:
            return
        name = m.group(1)
        start = pos + m.end()
        depth = 1
        i = start
        while i < len(text) and depth > 0:
            c = text[i]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            i += 1
        if depth != 0:
            return
        body = text[start : i - 1]
        yield name, body
        pos = i


def parse_fields(body: str):
    """Parse fields from a message body. Skips nested messages; flattens oneof
    (oneof fields are still real fields and we want them indexed)."""
    fields = []
    i = 0
    while i < len(body):
        # skip nested `message X { ... }` entirely
        m = re.match(r"\s*message\s+\w+\s*\{", body[i:])
        if m:
            j = i + m.end()
            depth = 1
            while j < len(body) and depth > 0:
                if body[j] == "{":
                    depth += 1
                elif body[j] == "}":
                    depth -= 1
                j += 1
            i = j
            continue
        # skip nested `enum X { ... }` (we don't track enum members as fields)
        m = re.match(r"\s*enum\s+\w+\s*\{", body[i:])
        if m:
            j = i + m.end()
            depth = 1
            while j < len(body) and depth > 0:
                if body[j] == "{":
                    depth += 1
                elif body[j] == "}":
                    depth -= 1
                j += 1
            i = j
            continue
        # `oneof X {` — strip wrapper, continue parsing inside
        m = re.match(r"\s*oneof\s+\w+\s*\{", body[i:])
        if m:
            i += m.end()
            continue
        # `reserved 1, 2, 3;` or `reserved "foo";` — skip
        m = re.match(r"\s*reserved\b[^;]*;", body[i:])
        if m:
            i += m.end()
            continue
        # `option (...) = ...;` — skip
        m = re.match(r"\s*option\b[^;]*;", body[i:])
        if m:
            i += m.end()
            continue
        # closing brace of oneof
        if body[i] == "}":
            i += 1
            continue
        # try to match a field statement up to next `;`. Span may include
        # `[...]` options across lines; brackets balanced.
        end = find_stmt_end(body, i)
        if end == -1:
            break
        stmt = body[i : end + 1]
        match = FIELD_RE.match(stmt)
        if match:
            rule = match.group("rule") or ""
            fields.append(
                {
                    "name": match.group("name"),
                    "type": match.group("type"),
                    "tag": int(match.group("tag")),
                    "repeated": rule == "repeated",
                }
            )
        i = end + 1
    return fields


def find_stmt_end(body: str, start: int) -> int:
    """Return index of the `;` that closes the statement starting at `start`,
    skipping any `;` inside `[ ... ]` option blocks. Returns -1 if not found."""
    i = start
    bracket = 0
    while i < len(body):
        c = body[i]
        if c == "[":
            bracket += 1
        elif c == "]":
            bracket -= 1
        elif c == ";" and bracket == 0:
            return i
        i += 1
    return -1


def extract_field_comments(text_with_comments: str) -> dict:
    """Walk the proto text line-by-line, tracking message scope, and return a
    map (innermost_message_name, field_name) → comment string.

    Comment association:
    - Consecutive `//` lines immediately preceding a field → joined with space.
    - A blank line between the comment and the field breaks the association.
    - Fallback: trailing `// ...` AFTER the `;` on the field's last line.
    """
    out: dict = {}
    msg_stack: list = []  # entries: short message name, or None for enum scopes
    pending: list[str] = []
    in_block_comment = False

    for raw_line in text_with_comments.split("\n"):
        stripped = raw_line.strip()

        # Block comment /* ... */ — drop, do NOT reset pending (it could be
        # before a `// ...` line followed by a field).
        if in_block_comment:
            if "*/" in stripped:
                in_block_comment = False
            continue
        if stripped.startswith("/*"):
            if "*/" not in stripped:
                in_block_comment = True
            continue

        if not stripped:
            pending = []
            continue
        if stripped.startswith("//"):
            pending.append(stripped.lstrip("/").strip())
            continue

        m_msg = re.match(r"^message\s+(\w+)\s*\{", stripped)
        m_enum = re.match(r"^enum\s+\w+\s*\{", stripped)
        m_oneof = re.match(r"^oneof\s+\w+\s*\{", stripped)

        if m_msg:
            msg_stack.append(m_msg.group(1))
            pending = []
            continue
        if m_enum:
            msg_stack.append(None)  # non-message scope
            pending = []
            continue
        if m_oneof:
            pending = []  # oneof doesn't change message context
            continue
        if stripped == "}":
            if msg_stack:
                msg_stack.pop()
            pending = []
            continue
        if not msg_stack or msg_stack[-1] is None:
            pending = []
            continue
        if stripped.startswith(("reserved", "option")):
            pending = []
            continue

        fm = re.match(
            r"^(?:repeated\s+|optional\s+|required\s+)?[\w.]+\s+(\w+)\s*=\s*\d+",
            stripped,
        )
        if fm:
            field_name = fm.group(1)
            # Find innermost message context (skip enum frames)
            innermost = None
            for s in reversed(msg_stack):
                if s is not None:
                    innermost = s
                    break
            if innermost:
                comment = " ".join(pending).strip()
                # Fallback: trailing `// ...` after the `;` on this line
                if not comment and ";" in raw_line and "//" in raw_line:
                    semi = raw_line.index(";")
                    rest = raw_line[semi + 1:]
                    if "//" in rest:
                        comment = rest.split("//", 1)[1].strip()
                if comment:
                    out[(innermost, field_name)] = comment
        pending = []
    return out


def extract_proto(path: Path, base: Path):
    raw = path.read_text()
    comments_map = extract_field_comments(raw)
    text = strip_comments(raw)
    pkg_match = PACKAGE_RE.search(text)
    package = pkg_match.group(1) if pkg_match else ""
    rel = str(path.relative_to(base))
    for name, body in find_top_level_messages(text):
        full = f"{package}.{name}" if package else name
        fields = parse_fields(body)
        for f in fields:
            c = comments_map.get((name, f["name"]), "")
            if c:
                f["comment"] = c
        yield full, {
            "file": rel,
            "fields": fields,
        }


def scan_namespace(
    root: Path, ns_subdir: str, source_label: str
) -> tuple[dict, list[tuple[str, str]]]:
    """Scan root/<ns_subdir>/**/*.proto. Return (messages, skipped)."""
    messages: dict[str, dict] = {}
    skipped: list[tuple[str, str]] = []
    target = root / ns_subdir
    if not target.exists():
        return messages, [(str(target), f"missing {ns_subdir}/ dir")]
    for proto in sorted(target.rglob("*.proto")):
        try:
            for full, msg in extract_proto(proto, root):
                if full in messages:
                    skipped.append((str(proto), f"duplicate name {full}"))
                    continue
                msg["source"] = source_label
                messages[full] = msg
        except Exception as e:  # noqa: BLE001
            skipped.append((str(proto), repr(e)))
    return messages, skipped


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: extract.py <poktroll_proto_dir>", file=sys.stderr)
        return 2
    poktroll_dir = Path(sys.argv[1])
    if not (poktroll_dir / "pocket").exists():
        print(f"no pocket/ dir under {poktroll_dir}", file=sys.stderr)
        return 3

    messages: dict[str, dict] = {}
    all_skipped: list[tuple[str, str]] = []

    # 1. poktroll-owned protos under pocket/
    pkt_msgs, pkt_skipped = scan_namespace(poktroll_dir, "pocket", "poktroll")
    messages.update(pkt_msgs)
    all_skipped.extend(pkt_skipped)

    # 2. cosmos-sdk protos (optional, when COSMOS_SDK_DIR env is set)
    csdk_dir = os.environ.get("COSMOS_SDK_DIR")
    if csdk_dir:
        csdk_path = Path(csdk_dir)
        for ns in ("cosmos", "tendermint", "amino"):
            ns_msgs, ns_skipped = scan_namespace(csdk_path, ns, "cosmos-sdk")
            # Conflict resolution: poktroll-owned wins over cosmos-sdk (shouldn't
            # happen in practice but defensive).
            for k, v in ns_msgs.items():
                if k not in messages:
                    messages[k] = v
            all_skipped.extend(ns_skipped)

    out = {
        "version": os.environ.get("VERSION_TAG", poktroll_dir.name.replace("_", ".")),
        "cosmos_sdk_version": os.environ.get("COSMOS_SDK_VERSION", ""),
        "vendored_at": os.environ.get("VENDORED_AT", "unknown"),
        "messages": messages,
    }
    json.dump(out, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")

    if all_skipped:
        print(f"WARN: skipped {len(all_skipped)} item(s):", file=sys.stderr)
        for path, reason in all_skipped[:20]:
            print(f"  - {path}: {reason}", file=sys.stderr)
        if len(all_skipped) > 20:
            print(f"  ... +{len(all_skipped) - 20} more", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
