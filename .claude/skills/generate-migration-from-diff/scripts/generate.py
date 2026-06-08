#!/usr/bin/env python3
"""Generate a goose migration SQL file for a poktroll version from its shape
snapshot. See SKILL.md for the design contract.

Usage:
  generate.py <vX.Y.Z>

Inputs (auto-located relative to repo root):
- docs/research/.shapes/<vX_Y_Z>.json       — current snapshot
- docs/research/.shapes/v*.json             — previous snapshots (semver-sorted)
- .claude/skills/generate-migration-from-diff/config.yaml — entity mappings

Output:
- schema/migrations/NNNN_decoder_<vX_Y_Z>.sql  — idempotent goose migration
"""

import json
import os
import re
import sys
from pathlib import Path

import yaml


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


# ────────────────────────────────────────────────────────────────────────────
# Helpers: paths, version math.
# ────────────────────────────────────────────────────────────────────────────


VERSION_DIR_RE = re.compile(r"v(\d+)_(\d+)_(\d+)")
VERSION_TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def tag_to_dir(tag: str) -> str:
    return tag.replace(".", "_")


def tag_to_id(tag: str) -> int:
    m = VERSION_TAG_RE.match(tag)
    if not m:
        raise ValueError(f"bad tag {tag!r}")
    x, y, z = (int(p) for p in m.groups())
    return x * 10000 + y * 100 + z


def semver_key_dir(name: str):
    m = VERSION_DIR_RE.fullmatch(name)
    return tuple(int(p) for p in m.groups()) if m else (0, 0, 0)


def next_migration_number(migrations_dir: Path) -> str:
    nums = []
    for p in migrations_dir.glob("*.sql"):
        m = re.match(r"(\d+)_", p.name)
        if m:
            nums.append(int(m.group(1)))
    return f"{(max(nums) + 1) if nums else 1:04d}"


# ────────────────────────────────────────────────────────────────────────────
# Proto type → SQL column derivation.
# ────────────────────────────────────────────────────────────────────────────


SCALAR_SQL = {
    "string": "TEXT",
    "bytes": "BYTEA",  # all observed uses are hashes/signatures/payloads/keys — opaque, no prefix queries
    "bool": "BOOLEAN",
    "uint64": "BIGINT",
    "int64": "BIGINT",
    "sint64": "BIGINT",
    "fixed64": "BIGINT",
    "sfixed64": "BIGINT",
    "uint32": "INTEGER",
    "int32": "INTEGER",
    "sint32": "INTEGER",
    "fixed32": "INTEGER",
    "sfixed32": "INTEGER",
    "double": "DOUBLE PRECISION",
    "float": "DOUBLE PRECISION",
}

COIN_TYPE = "cosmos.base.v1beta1.Coin"

# PostgreSQL reserved words that may appear as proto field names.
# Quoted in DDL + DML to avoid syntax errors. Case-insensitive matching.
PG_RESERVED = {
    "all", "analyze", "and", "any", "array", "as", "asc", "asymmetric",
    "authorization", "binary", "both", "case", "cast", "check", "collate",
    "collation", "column", "concurrently", "constraint", "create", "cross",
    "current_catalog", "current_date", "current_role", "current_schema",
    "current_time", "current_timestamp", "current_user", "default",
    "deferrable", "desc", "distinct", "do", "else", "end", "except",
    "false", "fetch", "for", "foreign", "freeze", "from", "full", "grant",
    "group", "having", "ilike", "in", "initially", "inner", "intersect",
    "into", "is", "isnull", "join", "lateral", "leading", "left", "like",
    "limit", "localtime", "localtimestamp", "natural", "not", "notnull",
    "null", "offset", "on", "only", "or", "order", "outer", "overlaps",
    "placing", "primary", "references", "returning", "right", "select",
    "session_user", "similar", "some", "symmetric", "table", "tablesample",
    "then", "to", "trailing", "true", "union", "unique", "user", "using",
    "variadic", "verbose", "when", "where", "window", "with",
}


def quote_ident(name: str) -> str:
    """Quote an identifier if it matches a PostgreSQL reserved word."""
    return f'"{name}"' if name.lower() in PG_RESERVED else name


def strip_denom_suffix(name: str, aliases: list[str]) -> str:
    for alias in aliases:
        suffix = f"_{alias}"
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def field_to_columns(
    field: dict, coin_aliases: list[str], coin_string_recognition: bool
) -> list[tuple[str, str, str]]:
    """Return list of (column_name, sql_type, comment) for a proto field.

    For Coin (or string-with-denom-alias-suffix when enabled), returns 2 cols
    with derived comments (amount / denom suffix).
    """
    name = field["name"]
    typ = field["type"]
    repeated = field["repeated"]
    comment = field.get("comment", "")

    # Coin → 2 cols. Suffix convention: <base>_amount + <base>_denom.
    # Exception: when base name is literally "amount" we collapse to
    # `amount` + `amount_denom` to avoid `amount_amount` redundancy.
    if typ == COIN_TYPE:
        base = strip_denom_suffix(name, coin_aliases)
        amt = base if base == "amount" else f"{base}_amount"
        return [
            (amt, "BIGINT", comment),
            (f"{base}_denom", "TEXT", comment),
        ]

    # string with denom alias suffix → treat as Coin (post-v0.1.27 wire shape)
    if coin_string_recognition and typ == "string":
        for alias in coin_aliases:
            if name.endswith(f"_{alias}"):
                base = strip_denom_suffix(name, coin_aliases)
                amt = base if base == "amount" else f"{base}_amount"
                return [
                    (amt, "BIGINT", comment),
                    (f"{base}_denom", "TEXT", comment),
                ]

    # repeated scalar → array
    if repeated and typ in SCALAR_SQL:
        return [(name, f"{SCALAR_SQL[typ]}[]", comment)]

    # repeated message → JSONB
    if repeated:
        return [(name, "JSONB", comment)]

    # scalar
    if typ in SCALAR_SQL:
        return [(name, SCALAR_SQL[typ], comment)]

    # message / enum / unknown → JSONB
    return [(name, "JSONB", comment)]


FRAMEWORK_COL_NAMES = {
    "block_height", "block_time", "tx_index", "event_index",
    "decoded_by_version", "indexed_at",
}


def entity_columns(
    entity_shape: dict, config: dict
) -> list[tuple[str, str, str]]:
    aliases = config.get("coin_denom_aliases", []) or []
    coin_string = bool(config.get("coin_string_recognition"))
    cols: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for field in entity_shape["fields"]:
        for col_name, col_type, col_comment in field_to_columns(
            field, aliases, coin_string
        ):
            # Framework column names are reserved — the framework column carries
            # the canonical value. A proto field with the same name is dropped
            # (its info would duplicate / collide with the framework column).
            if col_name in FRAMEWORK_COL_NAMES:
                continue
            if col_name in seen:
                continue
            seen.add(col_name)
            cols.append((col_name, col_type, col_comment))
    return cols


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def snake_case(name: str) -> str:
    """CamelCase → snake_case. EventClaimSettled → event_claim_settled."""
    out: list[str] = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0 and not name[i - 1].isupper():
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


def split_fqn(fqn: str) -> tuple[str, str, str]:
    """Split FQN into (module, version, short_name).

    Examples:
      pocket.tokenomics.EventClaimSettled → ("tokenomics", "", "EventClaimSettled")
      cosmos.bank.v1beta1.MsgSend → ("bank", "vb1", "MsgSend")
      cosmos.gov.v1.Proposal → ("gov", "v1", "Proposal")

    Version abbreviation: v1beta1 → vb1, v1alpha1 → va1 (Postgres 63-char limit).
    """
    parts = fqn.split(".")
    short = parts[-1]
    ver = ""
    for p in parts:
        m = re.match(r"^v(\d+)(beta|alpha)?(\d*)$", p)
        if m:
            base, mod, sub = m.group(1), m.group(2), m.group(3)
            if mod:
                ver = f"v{mod[0]}{sub or base}"  # v1beta1 → vb1 ; v2alpha → va2
            else:
                ver = f"v{base}"
            break
    module = parts[1] if len(parts) > 1 else ""
    return module, ver, short


def apply_auto_include(
    messages: dict, explicit: dict, rules: list[dict]
) -> dict:
    """Return merged entity config: explicit entities + auto-included.

    Explicit always wins. Rules are tried in order; first match wins.
    Auto-included entities are stateless (Event/Msg) with derived table name.
    """
    merged: dict = dict(explicit) if explicit else {}
    for fqn in messages:
        if fqn in merged:
            continue
        for rule in rules or []:
            if not re.match(rule["regex"], fqn):
                continue
            short = fqn.split(".")[-1]
            excl = rule.get("exclude_suffix")
            if excl and short.endswith(excl):
                break  # explicit exclusion — don't auto-include via this rule
            module, ver, sname = split_fqn(fqn)
            tbl = rule["table_template"]
            tbl = tbl.replace("{snake}", snake_case(sname))
            tbl = tbl.replace("{module}", module)
            tbl = tbl.replace("{ver}", ver) if ver else tbl.replace("_{ver}", "")
            # collapse double underscores in case of empty ver
            tbl = re.sub(r"_+", "_", tbl).strip("_")
            merged[fqn] = {
                "pattern": rule["pattern"],
                "table": tbl,
            }
            break
    return merged


def comment_on_column_sql(table: str, col: str, comment: str) -> str:
    if not comment:
        return ""
    return f"COMMENT ON COLUMN {table}.{quote_ident(col)} IS '{sql_escape(comment)}';\n"


# ────────────────────────────────────────────────────────────────────────────
# DDL generation.
# ────────────────────────────────────────────────────────────────────────────


FRAMEWORK_STATEFUL = [
    ("block_height", "BIGINT NOT NULL"),
    ("block_time", "TIMESTAMPTZ NOT NULL"),
    ("decoded_by_version", "SMALLINT NOT NULL"),
    ("indexed_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
]

FRAMEWORK_STATELESS = [
    ("block_height", "BIGINT NOT NULL"),
    ("block_time", "TIMESTAMPTZ NOT NULL"),
    ("tx_index", "INTEGER NOT NULL DEFAULT 0"),
    ("event_index", "INTEGER NOT NULL DEFAULT 0"),
    ("decoded_by_version", "SMALLINT NOT NULL"),
    ("indexed_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
]


def create_table_sql(
    entity_cfg: dict, body_cols: list[tuple[str, str, str]]
) -> str:
    table = entity_cfg["table"]
    pattern = entity_cfg["pattern"]
    parts: list[str] = []
    id_fields: list[str] = entity_cfg.get("id_fields", []) or []

    if pattern == "stateful":
        # id columns NOT NULL up front
        for idf in id_fields:
            parts.append(f"  {quote_ident(idf)} TEXT NOT NULL")
        for name, typ, _ in body_cols:
            if name in id_fields:
                continue
            parts.append(f"  {quote_ident(name)} {typ} NULL")
        for name, typ in FRAMEWORK_STATEFUL:
            parts.append(f"  {name} {typ}")
        pk_cols = ", ".join([quote_ident(c) for c in id_fields] + ["block_height"])
        parts.append(f"  CONSTRAINT {table}_pk PRIMARY KEY ({pk_cols})")
        parts.append(
            f"  CONSTRAINT {table}_decoder_fk "
            f"FOREIGN KEY (decoded_by_version) REFERENCES decoder_version(id)"
        )
    elif pattern == "singleton":
        for name, typ, _ in body_cols:
            parts.append(f"  {quote_ident(name)} {typ} NULL")
        for name, typ in FRAMEWORK_STATEFUL:
            parts.append(f"  {name} {typ}")
        parts.append(f"  CONSTRAINT {table}_pk PRIMARY KEY (block_height)")
        parts.append(
            f"  CONSTRAINT {table}_decoder_fk "
            f"FOREIGN KEY (decoded_by_version) REFERENCES decoder_version(id)"
        )
    else:  # stateless
        for name, typ, _ in body_cols:
            parts.append(f"  {quote_ident(name)} {typ} NULL")
        for name, typ in FRAMEWORK_STATELESS:
            parts.append(f"  {name} {typ}")
        # TimescaleDB requires the partition column (block_time) to be part of
        # every UNIQUE index, including PRIMARY KEY.
        parts.append(
            f"  CONSTRAINT {table}_pk PRIMARY KEY "
            f"(block_time, block_height, tx_index, event_index)"
        )
        parts.append(
            f"  CONSTRAINT {table}_decoder_fk "
            f"FOREIGN KEY (decoded_by_version) REFERENCES decoder_version(id)"
        )

    body = ",\n".join(parts)
    out = f"CREATE TABLE IF NOT EXISTS {table} (\n{body}\n);\n"
    if pattern == "stateful":
        idx_cols = ", ".join(
            [quote_ident(c) for c in id_fields] + ["block_height DESC"]
        )
        out += (
            f"CREATE INDEX IF NOT EXISTS {table}_current_idx "
            f"ON {table} ({idx_cols});\n"
        )
    elif pattern == "stateless":
        out += (
            f"SELECT create_hypertable('{table}', 'block_time', "
            "if_not_exists => TRUE);\n"
        )
    # COMMENT ON COLUMN for every body column that has a comment.
    for col_name, _, col_comment in body_cols:
        out += comment_on_column_sql(table, col_name, col_comment)
    return out


def alter_add_columns_sql(
    table: str, new_cols: list[tuple[str, str, str]]
) -> str:
    if not new_cols:
        return ""
    out = ""
    for name, typ, _ in new_cols:
        out += f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {quote_ident(name)} {typ} NULL;\n"
    for name, _, comment in new_cols:
        out += comment_on_column_sql(table, name, comment)
    return out


def comment_refresh_sql(
    table: str, refreshed: list[tuple[str, str]]
) -> str:
    """refreshed: [(col_name, new_comment), ...] — only columns whose comment changed."""
    if not refreshed:
        return ""
    out = "-- COMMENT refreshes (proto docstring changed since previous version)\n"
    for col, comment in refreshed:
        out += comment_on_column_sql(table, col, comment)
    return out


def drop_columns_sql(
    table: str, cols: list[tuple[str, str, str]]
) -> str:
    if not cols:
        return ""
    parts = [
        f"ALTER TABLE {table} DROP COLUMN IF EXISTS {quote_ident(name)};"
        for name, _, _ in cols
    ]
    return "\n".join(parts) + "\n"


# ────────────────────────────────────────────────────────────────────────────
# Orchestration.
# ────────────────────────────────────────────────────────────────────────────


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: generate.py <vX.Y.Z>", file=sys.stderr)
        return 2
    tag = sys.argv[1]
    if not VERSION_TAG_RE.match(tag):
        print(f"invalid tag: {tag}", file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    root = script_dir.parents[3]
    shapes_dir = root / "docs" / "research" / ".shapes"
    config_path = script_dir.parent / "config.yaml"
    migrations_dir = root / "schema" / "migrations"

    vdir = tag_to_dir(tag)
    cur_snapshot_path = shapes_dir / f"{vdir}.json"
    if not cur_snapshot_path.exists():
        print(
            f"missing snapshot: {cur_snapshot_path} — run /generate-decoder {tag} first",
            file=sys.stderr,
        )
        return 3

    cur = json.loads(cur_snapshot_path.read_text())
    cfg = load_yaml(config_path)
    explicit_entities = cfg.get("entities", {}) or {}
    auto_rules = cfg.get("auto_include", []) or []
    entities = apply_auto_include(cur["messages"], explicit_entities, auto_rules)

    # Find previous snapshot.
    cur_key = semver_key_dir(vdir)
    prev_path = None
    prev_key = (-1, -1, -1)
    for p in shapes_dir.glob("v*.json"):
        if p == cur_snapshot_path:
            continue
        k = semver_key_dir(p.stem)
        if k < cur_key and k > prev_key:
            prev_key = k
            prev_path = p
    prev = json.loads(prev_path.read_text()) if prev_path else None

    # Plan per-entity changes.
    create_blocks: list[str] = []
    alter_blocks: list[str] = []
    down_drops: list[str] = []
    down_alters: list[str] = []
    summary_rows: list[str] = []
    skipped: list[str] = []

    for entity_name, entity_cfg in entities.items():
        if entity_name not in cur["messages"]:
            skipped.append(f"{entity_name}: not present in {tag} snapshot")
            continue
        cur_cols = entity_columns(cur["messages"][entity_name], cfg)
        prev_exists = prev and entity_name in prev["messages"]
        if not prev_exists:
            sql = create_table_sql(entity_cfg, cur_cols)
            create_blocks.append(
                f"-- {entity_name} → {entity_cfg['table']} (first appearance)\n{sql}"
            )
            down_drops.append(f"DROP TABLE IF EXISTS {entity_cfg['table']} CASCADE;")
            summary_rows.append(
                f"   CREATE {entity_cfg['table']}: {len(cur_cols)} body cols"
            )
            continue

        prev_cols = entity_columns(prev["messages"][entity_name], cfg)
        prev_type = {c[0]: c[1] for c in prev_cols}
        prev_comment = {c[0]: c[2] for c in prev_cols}
        cur_type = {c[0]: c[1] for c in cur_cols}
        cur_comment = {c[0]: c[2] for c in cur_cols}

        added = [
            (n, t, c) for (n, t, c) in cur_cols if n not in prev_type
        ]
        removed = [
            (n, t, c) for (n, t, c) in prev_cols if n not in cur_type
        ]
        type_changed = [
            (n, prev_type[n], cur_type[n])
            for n in cur_type
            if n in prev_type and prev_type[n] != cur_type[n]
        ]
        comment_refreshed = [
            (n, cur_comment[n])
            for n in cur_type
            if n in prev_type
            and prev_type[n] == cur_type[n]
            and cur_comment.get(n, "") != prev_comment.get(n, "")
            and cur_comment.get(n, "")  # only refresh if new comment non-empty
        ]

        if not added and not removed and not type_changed and not comment_refreshed:
            summary_rows.append(f"   {entity_cfg['table']}: unchanged (skip)")
            continue

        block = [f"-- {entity_name} → {entity_cfg['table']} (additive)\n"]
        if added:
            block.append(alter_add_columns_sql(entity_cfg["table"], added))
        if comment_refreshed:
            block.append(
                comment_refresh_sql(entity_cfg["table"], comment_refreshed)
            )
        for name, _, _ in removed:
            block.append(
                f"-- REMOVED in {tag}: column {name} kept (append-only, ADR-005).\n"
            )
        for name, old_t, new_t in type_changed:
            block.append(
                f"-- TYPE CHANGE FLAG: {name} {old_t} → {new_t}. "
                "Verify decoder handles this; no destructive ALTER emitted.\n"
            )
        alter_blocks.append("".join(block))
        if added:
            down_alters.append(drop_columns_sql(entity_cfg["table"], added))
        summary_rows.append(
            f"   ALTER {entity_cfg['table']}: +{len(added)} cols, "
            f"-{len(removed)} commented, ~{len(type_changed)} flagged, "
            f"{len(comment_refreshed)} comment refresh(es)"
        )

    if not create_blocks and not alter_blocks:
        print(
            f"NOTE: nothing to do for {tag} — all tracked entities unchanged.",
            file=sys.stderr,
        )

    # Compose the file.
    nnnn = next_migration_number(migrations_dir)
    out_path = migrations_dir / f"{nnnn}_decoder_{vdir}.sql"

    decoder_id = tag_to_id(tag)
    header = f"""-- +goose Up
-- +goose StatementBegin

-- ─────────────────────────────────────────────────────────────────────────
-- Auto-generated by /generate-migration-from-diff for poktroll {tag}.
-- Idempotent. See .claude/skills/generate-migration-from-diff/SKILL.md.
-- Source snapshot: docs/research/.shapes/{vdir}.json
-- ─────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS decoder_version (
    id        SMALLINT PRIMARY KEY,
    tag       TEXT UNIQUE NOT NULL,
    added_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE decoder_version IS
    'Lookup of decoder packages. Populated by /generate-migration-from-diff. '
    'Referenced by every entity row via decoded_by_version.';

INSERT INTO decoder_version (id, tag) VALUES ({decoder_id}, '{tag}')
ON CONFLICT (tag) DO NOTHING;
"""

    body = "\n".join(create_blocks + alter_blocks)
    up = header + ("\n" + body if body else "")
    up += "\n-- +goose StatementEnd\n"

    down = "-- +goose Down\n-- +goose StatementBegin\n\n"
    for sql in down_alters:
        down += sql
    for sql in down_drops:
        down += sql + "\n"
    down += (
        f"DELETE FROM decoder_version WHERE tag = '{tag}';\n"
        "-- decoder_version table NOT dropped (other migrations depend on it).\n"
        "-- +goose StatementEnd\n"
    )

    out_path.write_text(up + "\n" + down)

    print(f"OK {tag} migration written.")
    print(f"   File: {out_path.relative_to(root)}")
    for row in summary_rows:
        print(row)
    if skipped:
        print("   Skipped:")
        for line in skipped:
            print(f"     - {line}")
    print(f"   decoder_version row: id={decoder_id}, tag={tag}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
