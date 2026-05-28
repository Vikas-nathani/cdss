"""
Verify that the master-schema logic in standardize_drug_source.py correctly
captures the shape of real DrugSourceMaster.clean_record records.

READ-ONLY: never writes to the database, never adds columns.

What it does:
  1. Fetch N sample rows from clean_record (default 50, --limit to change).
  2. Build the master schema from those rows using the SAME _skeleton/_merge
     functions used in the real script.
  3. For each sample row, overlay it onto the master template and verify:
       a) Every key/value in the original is preserved in the overlaid result
          (no data loss anywhere in the hierarchy).
       b) Every overlaid result has the exact same set of keys as the
          template (shape uniformity).
  4. Also dumps the first 2 raw rows to data/sample_rows.json and the derived
     schema to data/sample_master_schema.json for visual diffing.

Usage:
    python3 scripts/verify_schema.py                  # 50 rows
    python3 scripts/verify_schema.py --limit 500      # bigger sample
    python3 scripts/verify_schema.py --random         # random sample (TABLESAMPLE)
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from tqdm import tqdm

# Reuse the real merge/overlay logic so we're verifying the actual code path.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from standardize_drug_source import (  # noqa: E402
    DB_CONFIG,
    SOURCE_COLUMN,
    SOURCE_FILTER,
    TABLE,
    _UNSET,
    _finalize,
    _merge,
    _skeleton,
    overlay,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_ROWS_PATH = REPO_ROOT / "data" / "sample_rows.json"
SAMPLE_SCHEMA_PATH = REPO_ROOT / "data" / "sample_master_schema.json"


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------

def find_data_loss(original, overlaid, path: str = "") -> list[str]:
    """Return a list of JSON-path strings where `original` has a value that is
    missing or different in `overlaid`. Empty list = no data loss."""
    losses: list[str] = []

    if isinstance(original, dict):
        if not isinstance(overlaid, dict):
            return [f"{path or '<root>'}: original=dict, overlaid={type(overlaid).__name__}"]
        for k, v in original.items():
            sub = f"{path}.{k}" if path else k
            if k not in overlaid:
                losses.append(f"{sub}: missing from overlaid")
                continue
            losses.extend(find_data_loss(v, overlaid[k], sub))
        return losses

    if isinstance(original, list):
        if not isinstance(overlaid, list):
            return [f"{path}: original=list, overlaid={type(overlaid).__name__}"]
        if len(original) != len(overlaid):
            losses.append(f"{path}: length original={len(original)} overlaid={len(overlaid)}")
            return losses
        for i, (a, b) in enumerate(zip(original, overlaid)):
            losses.extend(find_data_loss(a, b, f"{path}[{i}]"))
        return losses

    # scalar
    if original != overlaid:
        losses.append(f"{path}: original={original!r} overlaid={overlaid!r}")
    return losses


def collect_paths(value, path: str = "") -> set[str]:
    """All key-paths in a nested structure (lists collapse to '[]')."""
    paths: set[str] = set()
    if isinstance(value, dict):
        for k, v in value.items():
            sub = f"{path}.{k}" if path else k
            paths.add(sub)
            paths.update(collect_paths(v, sub))
    elif isinstance(value, list) and value:
        # representative element
        paths.update(collect_paths(value[0], f"{path}[]"))
    return paths


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_record(rec: Any) -> Any:
    if rec is None:
        return None
    if isinstance(rec, str):
        try:
            rec = json.loads(rec)
        except json.JSONDecodeError:
            return None
    return rec if isinstance(rec, dict) else None


def run_full_scan() -> int:
    """Stream every row in the table, build the schema, then stream again
    and verify each row. No row data is buffered."""
    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected (read-only verification — no writes will happen).")

    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE source = %s", (SOURCE_FILTER,))
            total = cur.fetchone()[0]
        print(f"source='{SOURCE_FILTER}' has {total:,} rows. Doing two streaming passes:")

        # ---- Pass 1: build the master schema from every row ----
        print(f"\nPass 1/2: building master schema from all source='{SOURCE_FILTER}' rows ...")
        master: Any = _UNSET
        union_top_keys: set[str] = set()
        skipped_p1 = 0
        with conn.cursor(name="verify_p1",
                         cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.itersize = 2000
            cur.execute(f"SELECT {SOURCE_COLUMN} FROM {TABLE} WHERE source = %s", (SOURCE_FILTER,))
            for row in tqdm(cur, total=total, unit="rows"):
                rec = _parse_record(row[SOURCE_COLUMN])
                if rec is None:
                    skipped_p1 += 1
                    continue
                union_top_keys |= set(rec.keys())
                sk = _skeleton(rec)
                master = sk if master is _UNSET else _merge(master, sk)
        master = _finalize(master)
        if not isinstance(master, dict):
            print("ERROR: top-level of clean_record is not an object.")
            return 1
        template_keys = set(master.keys())
        template_paths = collect_paths(master)
        print(f"  Schema: {len(template_keys)} top-level keys, "
              f"{len(template_paths)} total paths "
              f"(skipped {skipped_p1} unparseable rows).")

        # dump artefacts
        SAMPLE_SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(SAMPLE_SCHEMA_PATH, "w") as f:
            json.dump(master, f, indent=2)
        print(f"  Schema written → {SAMPLE_SCHEMA_PATH}")

        # ---- Pass 2: verify each row against the master ----
        print(f"\nPass 2/2: verifying every source='{SOURCE_FILTER}' row against the master schema ...")
        data_loss_count = 0
        shape_mismatch_count = 0
        rows_checked = 0
        first_loss: list[str] = []
        first_shape: tuple = ()
        with conn.cursor(name="verify_p2",
                         cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.itersize = 2000
            cur.execute(f"SELECT {SOURCE_COLUMN} FROM {TABLE} WHERE source = %s", (SOURCE_FILTER,))
            for i, row in enumerate(tqdm(cur, total=total, unit="rows")):
                rec = _parse_record(row[SOURCE_COLUMN])
                if rec is None:
                    continue
                rows_checked += 1
                result = overlay(copy.deepcopy(master), rec)
                losses = find_data_loss(rec, result)
                if losses:
                    data_loss_count += 1
                    if not first_loss:
                        first_loss = losses[:5] + [
                            f"(row #{i}, set_id={rec.get('set_id')!r})"
                        ]
                if set(result.keys()) != template_keys:
                    shape_mismatch_count += 1
                    if not first_shape:
                        first_shape = (
                            template_keys - set(result.keys()),
                            set(result.keys()) - template_keys,
                            i,
                        )
    finally:
        conn.close()

    return _print_report(
        rows_checked, union_top_keys, template_keys,
        data_loss_count, first_loss, shape_mismatch_count, first_shape,
        rows_with_all_top_keys=None,  # too expensive to track in streaming mode
    )


def _print_report(rows_checked: int,
                  union_top_keys: set[str],
                  template_keys: set[str],
                  data_loss_count: int,
                  first_loss: list[str],
                  shape_mismatch_count: int,
                  first_shape: tuple,
                  rows_with_all_top_keys: int | None) -> int:
    print("\n" + "=" * 60)
    print("VERIFICATION REPORT")
    print("=" * 60)
    print(f"Rows checked:                       {rows_checked:,}")
    print(f"Distinct top-level keys (union):    {len(union_top_keys)}")
    print(f"Master template top-level keys:     {len(template_keys)}")
    if union_top_keys != template_keys:
        print(f"  !! MISMATCH between union and template — bug in merge logic")
        print(f"     in union not template: {union_top_keys - template_keys}")
        print(f"     in template not union: {template_keys - union_top_keys}")
    else:
        print(f"  OK — template covers exactly the union of all rows.")

    if rows_with_all_top_keys is not None:
        print(f"\nRows whose own top-level keys ==    {rows_with_all_top_keys} / {rows_checked}")
        print(f"  template (i.e. already complete)")

    print(f"\nData-loss check                     {data_loss_count} rows lost data")
    if data_loss_count == 0:
        print(f"  OK — overlay preserved all data in all {rows_checked:,} rows.")
    else:
        print(f"  !! FAIL — first example:")
        for line in first_loss:
            print(f"     {line}")

    print(f"\nShape-uniformity check              {shape_mismatch_count} rows differ")
    if shape_mismatch_count == 0:
        print(f"  OK — all {rows_checked:,} overlaid rows share the exact same shape.")
    else:
        missing, extra, idx = first_shape
        print(f"  !! FAIL — row #{idx}: missing={missing}, extra={extra}")

    print()
    ok = (
        union_top_keys == template_keys
        and data_loss_count == 0
        and shape_mismatch_count == 0
    )
    if ok:
        print("ALL CHECKS PASSED — schema logic is correct against your data.")
        return 0
    print("ONE OR MORE CHECKS FAILED — see details above.")
    return 2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="Sample size (default 50).")
    parser.add_argument("--random", action="store_true",
                        help="Use TABLESAMPLE for a random sample instead of LIMIT.")
    parser.add_argument("--all", action="store_true",
                        help="Verify EVERY row in the table (two streaming passes). "
                             "Read-only. Ignores --limit/--random.")
    args = parser.parse_args()

    if args.all:
        return run_full_scan()

    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected (read-only verification — no writes will happen).")

    if args.random:
        sql = (
            f"SELECT {SOURCE_COLUMN} FROM {TABLE} "
            f"TABLESAMPLE BERNOULLI(1) WHERE source = '{SOURCE_FILTER}' LIMIT {int(args.limit)}"
        )
    else:
        sql = (
            f"SELECT {SOURCE_COLUMN} FROM {TABLE} "
            f"WHERE source = '{SOURCE_FILTER}' LIMIT {int(args.limit)}"
        )

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print("No rows returned — table is empty?")
        return 1

    records: list[dict] = []
    skipped = 0
    for row in rows:
        rec = row[SOURCE_COLUMN]
        if rec is None:
            skipped += 1
            continue
        if isinstance(rec, str):
            try:
                rec = json.loads(rec)
            except json.JSONDecodeError:
                skipped += 1
                continue
        if not isinstance(rec, dict):
            skipped += 1
            continue
        records.append(rec)

    print(f"\nFetched {len(rows)} rows, kept {len(records)} valid JSON objects "
          f"(skipped {skipped}).")

    # ---- build schema from the sample ----
    master = _UNSET
    for rec in records:
        sk = _skeleton(rec)
        master = sk if master is _UNSET else _merge(master, sk)
    master = _finalize(master)
    assert isinstance(master, dict)

    # dump artefacts for visual diffing
    SAMPLE_ROWS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SAMPLE_ROWS_PATH, "w") as f:
        json.dump(records[:2], f, indent=2)
    with open(SAMPLE_SCHEMA_PATH, "w") as f:
        json.dump(master, f, indent=2)
    print(f"Dumped first 2 raw rows  → {SAMPLE_ROWS_PATH}")
    print(f"Dumped derived schema    → {SAMPLE_SCHEMA_PATH}")

    template_paths = collect_paths(master)
    template_keys = set(master.keys())
    print(f"\nMaster schema (from sample): "
          f"{len(template_keys)} top-level keys, {len(template_paths)} total paths.")

    # ---- per-row checks ----
    data_loss_count = 0
    shape_mismatch_count = 0
    rows_with_all_top_keys = 0
    union_top_keys: set[str] = set()
    first_loss_example: list[str] = []
    first_shape_example: tuple = ()

    for i, rec in enumerate(records):
        rec_top = set(rec.keys())
        union_top_keys |= rec_top
        if rec_top == template_keys:
            rows_with_all_top_keys += 1

        result = overlay(copy.deepcopy(master), rec)

        losses = find_data_loss(rec, result)
        if losses:
            data_loss_count += 1
            if not first_loss_example:
                first_loss_example = losses[:5]
                first_loss_example.append(f"(row #{i}, set_id={rec.get('set_id')!r})")

        if set(result.keys()) != template_keys:
            shape_mismatch_count += 1
            if not first_shape_example:
                first_shape_example = (
                    template_keys - set(result.keys()),
                    set(result.keys()) - template_keys,
                    i,
                )

    # ---- report ----
    print("\n" + "=" * 60)
    print("VERIFICATION REPORT")
    print("=" * 60)
    print(f"Rows checked:                       {len(records)}")
    print(f"Distinct top-level keys (union):    {len(union_top_keys)}")
    print(f"Master template top-level keys:     {len(template_keys)}")
    if union_top_keys != template_keys:
        print(f"  !! MISMATCH between union and template — bug in merge logic")
        print(f"     in union not template: {union_top_keys - template_keys}")
        print(f"     in template not union: {template_keys - union_top_keys}")
    else:
        print(f"  OK — template covers exactly the union of all sampled rows.")

    print(f"\nRows whose own top-level keys ==    {rows_with_all_top_keys} / {len(records)}")
    print(f"  template (i.e. already complete)")
    print(f"  (the rest have a SUBSET — overlay will fill missing keys with null)")

    print(f"\nData-loss check (every value in     {data_loss_count} rows lost data")
    print(f"  the original survives the overlay):")
    if data_loss_count == 0:
        print(f"  OK — overlay preserved all data in all {len(records)} rows.")
    else:
        print(f"  !! FAIL — first example:")
        for line in first_loss_example:
            print(f"     {line}")

    print(f"\nShape-uniformity check (overlaid    {shape_mismatch_count} rows differ")
    print(f"  rows have identical top-level keys):")
    if shape_mismatch_count == 0:
        print(f"  OK — all {len(records)} overlaid rows share the exact same shape.")
    else:
        missing, extra, idx = first_shape_example
        print(f"  !! FAIL — row #{idx}: missing={missing}, extra={extra}")

    print()
    ok = (
        union_top_keys == template_keys
        and data_loss_count == 0
        and shape_mismatch_count == 0
    )
    if ok:
        print("ALL CHECKS PASSED — schema logic is correct against your data.")
        return 0
    print("ONE OR MORE CHECKS FAILED — see details above.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
