#!/usr/bin/env python3
"""Merge override.toml on top of base.toml, writing to out.toml.

Reads with stdlib `tomllib` (Python 3.11+). Writes with a hand-rolled
minimal TOML emitter that handles only what Cosmos SDK app.toml needs:
    - top-level scalars (str, int, float, bool)
    - tables  [section]
    - subtables [section.subsection]
    - arrays of scalars (e.g. streaming.abci.keys)

No external dependencies. Avoids yq's inline-table emission bug for
multi-section TOML.

Usage:
    merge-toml.py <base> <override> <out>
"""
import sys
import tomllib


def deep_merge(base, override):
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def fmt(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        # Use double-quoted strings; escape inner double quotes and backslashes
        esc = v.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{esc}"'
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, list):
        return "[" + ", ".join(fmt(x) for x in v) + "]"
    raise TypeError(f"unsupported TOML value type: {type(v).__name__}")


def emit(doc, out, prefix=""):
    """Emit a dict as TOML. Scalars + arrays first, then tables."""
    # Split entries into scalars (incl. arrays) and tables
    scalars = []
    tables = []
    for k, v in doc.items():
        if isinstance(v, dict):
            tables.append((k, v))
        else:
            scalars.append((k, v))

    # Emit scalars at current scope
    for k, v in scalars:
        out.write(f"{k} = {fmt(v)}\n")
    if scalars and tables:
        out.write("\n")

    # Emit tables as proper [section] headers (recursively)
    for k, v in tables:
        section = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
        out.write(f"[{section}]\n")
        emit(v, out, section)
        out.write("\n")


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    base_path, ov_path, out_path = sys.argv[1:]
    with open(base_path, "rb") as f:
        base = tomllib.load(f)
    with open(ov_path, "rb") as f:
        override = tomllib.load(f)
    merged = deep_merge(base, override)
    with open(out_path, "w") as f:
        emit(merged, f)


if __name__ == "__main__":
    main()
