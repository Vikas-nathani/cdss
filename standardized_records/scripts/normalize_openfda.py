"""
Standardize DrugSourceMaster.clean_record into a uniform JSONB shape.

Phase 1: scan every row's `clean_record`, recursively merge the structures
         into a single hierarchical master schema (all leaves = null), and
         save it to data/master_schema.json.

Phase 2: add a `standardized_data` JSONB column to DrugSourceMaster and,
         for every row, overlay the row's `clean_record` onto the master
         template so all rows share the exact same shape.

Usage:
    python scripts/standardize_drug_source.py            # interactive: phase 1, prompt, phase 2
    python scripts/standardize_drug_source.py --phase 1  # phase 1 only
    python scripts/standardize_drug_source.py --phase 2  # phase 2 only (requires existing master_schema.json)
    python scripts/standardize_drug_source.py --smoke    # 2-record smoke test, no writes
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from tqdm import tqdm


DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
    "dbname": os.getenv("PG_DB", "postgres"),
}

TABLE = '"DrugSourceMaster"'
SOURCE_COLUMN = "clean_record"
TARGET_COLUMN = "standardized_data"
SOURCE_FILTER = "openfda"   # only process rows WHERE source = this value

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "data" / "master_schema.json"

READ_BATCH = 2000
WRITE_BATCH = 500


# ---------------------------------------------------------------------------
# Schema merging — preserves hierarchy
#
# Three sentinels distinguish what we've observed for a given path:
#   _UNSET   — never seen (null in source, or no info yet)
#   _SCALAR  — seen as scalar (string/number/bool) in at least one row
#   dict/list — seen as a structured value
# After merging we _finalize() to plain JSON (sentinels → None).
# ---------------------------------------------------------------------------

class _Sentinel:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:  # pragma: no cover
        return f"<{self.name}>"


_UNSET = _Sentinel("unset")
_SCALAR = _Sentinel("scalar")


def _skeleton(value: Any) -> Any:
    """Reduce a JSON value to a hierarchical skeleton.
    Scalars become _SCALAR, nulls become _UNSET, structures recurse."""
    if value is None:
        return _UNSET
    if isinstance(value, dict):
        return {k: _skeleton(v) for k, v in value.items()}
    if isinstance(value, list):
        if not value:
            return []  # empty list — no info about element type
        elem: Any = _UNSET
        for item in value:
            sk = _skeleton(item)
            elem = sk if elem is _UNSET else _merge(elem, sk)
        if isinstance(elem, dict):
            return [elem]
        # list had non-dict items — record that fact so a later merge with
        # a list-of-dicts collapses the per-element template to "[]".
        return [_SCALAR]
    # bool / int / float / str / etc.
    return _SCALAR


def _merge(a: Any, b: Any) -> Any:
    """Recursively merge two skeletons. Mixed types collapse to _SCALAR
    (overlay treats _SCALAR/None as 'pass through whatever the row had')."""
    if a is _UNSET:
        return b
    if b is _UNSET:
        return a
    if a is _SCALAR and b is _SCALAR:
        return _SCALAR
    if a is _SCALAR or b is _SCALAR:
        # scalar in one row, structured in another — can't reconcile structure;
        # collapse to scalar slot so we don't drop data on overlay.
        return _SCALAR
    if isinstance(a, dict) and isinstance(b, dict):
        out = dict(a)
        for k, v in b.items():
            out[k] = _merge(a[k], v) if k in a else v
        return out
    if isinstance(a, list) and isinstance(b, list):
        a_elem = a[0] if a else _UNSET
        b_elem = b[0] if b else _UNSET
        merged_elem = _merge(a_elem, b_elem)
        if isinstance(merged_elem, dict):
            return [merged_elem]
        if merged_elem is _UNSET:
            return []  # both empty / no info either way
        # Element was non-dict in at least one row. Keep [_SCALAR] sticky so
        # a later merge against a [{dict}] template can't re-promote the slot.
        return [_SCALAR]
    # dict-vs-list mismatch — fall back to scalar slot
    return _SCALAR


def _finalize(template: Any) -> Any:
    """Convert sentinels to JSON-safe None for serialization."""
    if template is _UNSET or template is _SCALAR:
        return None
    if isinstance(template, dict):
        return {k: _finalize(v) for k, v in template.items()}
    if isinstance(template, list):
        if template and isinstance(template[0], dict):
            return [_finalize(template[0])]
        return []
    return template


# ---------------------------------------------------------------------------
# Overlay — apply real data onto the master template
# ---------------------------------------------------------------------------

def overlay(template: Any, data: Any) -> Any:
    """Return a value shaped like `template`, with values from `data` where
    present. The template only constrains *keys* — when the row's value type
    disagrees with the template's expected type, we preserve the row's value
    rather than drop it."""
    if isinstance(template, dict):
        src = data if isinstance(data, dict) else {}
        return {key: overlay(tmpl_val, src.get(key)) for key, tmpl_val in template.items()}
    if isinstance(template, list):
        if isinstance(data, list):
            if template and isinstance(template[0], dict):
                element_tmpl = template[0]
                return [overlay(element_tmpl, item) for item in data]
            return list(data)
        # row had a non-list value where the schema expects a list — keep it.
        return data if data is not None else []
    # template is None (scalar slot) — pass the row's value through unchanged.
    return data


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def connect():
    return psycopg2.connect(**DB_CONFIG)


def detect_primary_key(conn) -> str:
    """Return the PK column name for DrugSourceMaster."""
    sql = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (TABLE,))
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError(
            f"Could not detect primary key on {TABLE}. "
            "Pass one explicitly by editing PK_OVERRIDE in this script."
        )
    if len(rows) > 1:
        raise RuntimeError(
            f"Composite PK detected on {TABLE} ({[r[0] for r in rows]}). "
            "This script assumes a single-column PK."
        )
    return rows[0][0]


def row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE source = %s", (SOURCE_FILTER,))
        return cur.fetchone()[0]


def column_exists(conn, column: str) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """
    # information_schema lowercases unquoted names; the table was created as
    # "DrugSourceMaster" so it stays mixed-case in the catalog.
    with conn.cursor() as cur:
        cur.execute(sql, ("DrugSourceMaster", column))
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------

def smoke_test(conn) -> None:
    print(f"Smoke test: fetching 2 rows from {TABLE}.{SOURCE_COLUMN} (source='{SOURCE_FILTER}') ...")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"SELECT {SOURCE_COLUMN} FROM {TABLE} WHERE source = %s LIMIT 2", (SOURCE_FILTER,))
        rows = cur.fetchall()
    if not rows:
        print("  Table is empty — nothing to do.")
        return
    print(f"  Got {len(rows)} rows. Top-level keys per row:")
    for i, row in enumerate(rows, 1):
        rec = row[SOURCE_COLUMN] or {}
        if isinstance(rec, str):
            rec = json.loads(rec)
        keys = list(rec.keys()) if isinstance(rec, dict) else f"<{type(rec).__name__}>"
        print(f"    row {i}: {keys}")
    preview: Any = _UNSET
    for row in rows:
        rec = row[SOURCE_COLUMN] or {}
        if isinstance(rec, str):
            rec = json.loads(rec)
        sk = _skeleton(rec)
        preview = sk if preview is _UNSET else _merge(preview, sk)
    print("  Preview skeleton from these 2 rows:")
    print("    " + json.dumps(_finalize(preview), indent=2, sort_keys=False).replace("\n", "\n    "))


def build_master_schema(conn) -> dict:
    total = row_count(conn)
    print(f"Phase 1: scanning {total:,} rows (source='{SOURCE_FILTER}') to build master schema ...")
    master: Any = _UNSET

    # server-side named cursor → streams rows, doesn't buffer them all
    with conn.cursor(name="schema_scan", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = READ_BATCH
        cur.execute(f"SELECT {SOURCE_COLUMN} FROM {TABLE} WHERE source = %s", (SOURCE_FILTER,))
        for row in tqdm(cur, total=total, unit="rows"):
            rec = row[SOURCE_COLUMN]
            if rec is None:
                continue
            if isinstance(rec, str):
                try:
                    rec = json.loads(rec)
                except json.JSONDecodeError:
                    continue
            sk = _skeleton(rec)
            master = sk if master is _UNSET else _merge(master, sk)

    if master is _UNSET:
        raise RuntimeError("No JSON records found — cannot derive a schema.")
    if not isinstance(master, dict):
        raise RuntimeError(
            f"Top-level of clean_record is not an object (got {type(master).__name__}). "
            "This script assumes records are JSON objects."
        )
    return _finalize(master)


def save_schema(schema: dict) -> None:
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SCHEMA_PATH, "w") as f:
        json.dump(schema, f, indent=2, sort_keys=False)
    print(f"Phase 1: master schema saved → {SCHEMA_PATH}")
    print(f"         top-level keys: {list(schema.keys())}")


def load_schema() -> dict:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"{SCHEMA_PATH} not found — run phase 1 first.")
    with open(SCHEMA_PATH) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------

def ensure_target_column(conn) -> None:
    if column_exists(conn, TARGET_COLUMN):
        print(f"Phase 2: column {TARGET_COLUMN} already exists.")
        return
    print(f"Phase 2: adding column {TARGET_COLUMN} JSONB ...")
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {TARGET_COLUMN} JSONB")
    conn.commit()


def populate(conn, template: dict, pk_col: str) -> None:
    total = row_count(conn)
    print(f"Phase 2: populating {TARGET_COLUMN} for {total:,} rows (source='{SOURCE_FILTER}', batch={WRITE_BATCH}) ...")

    update_sql = (
        f'UPDATE {TABLE} AS t '
        f'SET {TARGET_COLUMN} = v.payload::jsonb '
        f'FROM (VALUES %s) AS v(pk, payload) '
        f'WHERE t."{pk_col}" = v.pk'
    )

    read_conn = connect()
    try:
        with read_conn.cursor(name="populate_scan",
                              cursor_factory=psycopg2.extras.RealDictCursor) as read_cur, \
             conn.cursor() as write_cur:
            read_cur.itersize = READ_BATCH
            read_cur.execute(
                f'SELECT "{pk_col}" AS pk, {SOURCE_COLUMN} FROM {TABLE} WHERE source = %s',
                (SOURCE_FILTER,),
            )

            buf: list[tuple] = []
            pbar = tqdm(total=total, unit="rows")
            for row in read_cur:
                rec = row[SOURCE_COLUMN]
                if isinstance(rec, str):
                    try:
                        rec = json.loads(rec)
                    except json.JSONDecodeError:
                        rec = {}
                if rec is None:
                    rec = {}
                merged = overlay(copy.deepcopy(template), rec)
                buf.append((row["pk"], json.dumps(merged)))
                if len(buf) >= WRITE_BATCH:
                    psycopg2.extras.execute_values(write_cur, update_sql, buf, page_size=WRITE_BATCH)
                    conn.commit()
                    pbar.update(len(buf))
                    buf.clear()
            if buf:
                psycopg2.extras.execute_values(write_cur, update_sql, buf, page_size=WRITE_BATCH)
                conn.commit()
                pbar.update(len(buf))
            pbar.close()
    finally:
        read_conn.close()
    print("Phase 2: done.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", type=int, choices=[1, 2], default=None,
                        help="Run only phase 1 or phase 2 (default: both, with confirmation between).")
    parser.add_argument("--smoke", action="store_true",
                        help="Smoke-test only: read 2 rows, print preview, exit. No writes.")
    parser.add_argument("--yes", action="store_true",
                        help="Skip the confirmation prompt before phase 2.")
    args = parser.parse_args()

    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} as {DB_CONFIG['user']} ...")
    conn = connect()
    print("Connected.")

    try:
        smoke_test(conn)
        if args.smoke:
            return 0

        if args.phase in (None, 1):
            schema = build_master_schema(conn)
            save_schema(schema)
        else:
            schema = load_schema()
            print(f"Phase 2: loaded existing master schema from {SCHEMA_PATH}")

        if args.phase == 1:
            return 0

        if not args.yes:
            resp = input("Proceed to Phase 2 (add column + populate 250K rows)? [y/N] ").strip().lower()
            if resp != "y":
                print("Aborted before Phase 2.")
                return 0

        pk_col = detect_primary_key(conn)
        print(f"Phase 2: detected primary key column = {pk_col!r}")
        ensure_target_column(conn)
        populate(conn, schema, pk_col)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
