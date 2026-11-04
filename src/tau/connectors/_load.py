"""Shared helpers for connector ``load`` implementations."""

from __future__ import annotations

LOAD_MODES = ("append", "replace", "upsert")


def check_load_mode(mode: str, merge_key: str | list[str] | None) -> list[str]:
    """Validate ``mode``/``merge_key`` and return the merge key as a list of columns."""
    if mode not in LOAD_MODES:
        raise ValueError(f"Unknown load mode '{mode}'. Use one of: {', '.join(LOAD_MODES)}")
    if mode == "upsert" and not merge_key:
        raise ValueError("mode='upsert' needs merge_key (the column(s) that identify a row)")
    if not merge_key:
        return []
    if isinstance(merge_key, str):
        return [k.strip() for k in merge_key.split(",") if k.strip()]
    return list(merge_key)


def key_delete_sql(table: str, keys: list[str], rows: int, placeholder) -> tuple[str, int]:
    """``DELETE FROM table WHERE (k1, k2) IN ((?, ?), ...)`` for ``rows`` rows.

    ``placeholder(i)`` renders the i-th (0-based) parameter, e.g. ``lambda i: f"${i+1}"``.
    Returns the SQL and the number of parameters it uses.
    """
    n = 0
    groups = []
    for _ in range(rows):
        cells = []
        for _ in keys:
            cells.append(placeholder(n))
            n += 1
        groups.append(f"({', '.join(cells)})" if len(keys) > 1 else cells[0])
    target = f"({', '.join(keys)})" if len(keys) > 1 else keys[0]
    return f"DELETE FROM {table} WHERE {target} IN ({', '.join(groups)})", n
