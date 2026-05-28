#!/usr/bin/env python3
"""
manual_rxcui_patch.py — Apply manually curated RxCUI mappings to indian_brand_ingredient.

Reads ~/cdss/data/ingredients_rxcui.csv. For each row with a non-empty rxcui,
updates indian_brand_ingredient SET rxcui_in = <rxcui>, match_confidence = 'manual'
WHERE LOWER(ingredient_name_norm) = LOWER(<name>) AND rxcui_in IS NULL.

Usage:
    python manual_rxcui_patch.py [--dry-run] [--password TEXT]
"""

import argparse
import csv
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

_env_path = Path(__file__).parent.parent.parent / ".env"
if _env_path.exists():
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k not in os.environ:
                    os.environ[_k] = _v

import psycopg2

from utils import connect_db

CSV_PATH = Path(__file__).parent.parent.parent / "data" / "ingredients_rxcui.csv"


def load_csv(path: Path):
    """Return list of (ingredient_name_norm, rxcui) for rows with a non-empty rxcui."""
    entries = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rxcui = row.get("rxcui", "").strip()
            name = row.get("ingredient_name_norm", "").strip()
            if rxcui and name:
                entries.append((name, rxcui))
    return entries


def dry_run_check(conn, entries):
    """For each entry, show how many rows would be updated."""
    print(f"\n{'='*60}")
    print(f"DRY-RUN: Checking {len(entries)} entries with mapped RxCUI")
    print(f"{'='*60}")
    total_would_update = 0
    total_already_set = 0
    total_no_match = 0
    with conn.cursor() as cur:
        for name, rxcui in entries:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE rxcui_in IS NULL) AS will_update,
                    COUNT(*) FILTER (WHERE rxcui_in IS NOT NULL) AS already_set
                FROM drugdb.indian_brand_ingredient
                WHERE LOWER(ingredient_name_norm) = LOWER(%s)
                """,
                (name,),
            )
            row = cur.fetchone()
            will_update, already_set = row
            if will_update > 0:
                print(f"  WILL UPDATE ({will_update} rows): '{name}' → rxcui={rxcui}")
                total_would_update += will_update
            elif already_set > 0:
                total_already_set += already_set
            else:
                print(f"  NO MATCH in DB: '{name}'")
                total_no_match += 1

    print(f"\n--- Summary ---")
    print(f"  Rows that would be updated:  {total_would_update}")
    print(f"  Rows already have rxcui_in:  {total_already_set}")
    print(f"  CSV names not found in DB:   {total_no_match}")
    print(f"  (re-run without --dry-run to apply)")


def apply_updates(conn, entries):
    """Apply updates and report results."""
    print(f"\n{'='*60}")
    print(f"Applying {len(entries)} manually curated RxCUI mappings")
    print(f"{'='*60}")
    total_updated = 0
    total_already_set = 0
    total_no_match = 0

    with conn.cursor() as cur:
        for name, rxcui in entries:
            cur.execute(
                """
                UPDATE drugdb.indian_brand_ingredient
                SET rxcui_in = %s, match_confidence = 'manual'
                WHERE LOWER(ingredient_name_norm) = LOWER(%s)
                  AND rxcui_in IS NULL
                """,
                (rxcui, name),
            )
            rows_updated = cur.rowcount

            if rows_updated > 0:
                print(f"  Updated {rows_updated} row(s): '{name}' → rxcui={rxcui}")
                total_updated += rows_updated
            else:
                cur.execute(
                    "SELECT COUNT(*) FROM drugdb.indian_brand_ingredient WHERE LOWER(ingredient_name_norm) = LOWER(%s)",
                    (name,),
                )
                count = cur.fetchone()[0]
                if count > 0:
                    total_already_set += count
                else:
                    print(f"  NO MATCH in DB: '{name}'")
                    total_no_match += 1

    conn.commit()

    print(f"\n--- Summary ---")
    print(f"  Rows updated (rxcui_in set): {total_updated}")
    print(f"  Rows already had rxcui_in:   {total_already_set}")
    print(f"  CSV names not found in DB:   {total_no_match}")


def parse_args():
    p = argparse.ArgumentParser(description="Manual RxCUI patch")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would change without committing.")
    p.add_argument("--password", type=str, default=None,
                   help="DB password override.")
    return p.parse_args()


def main():
    args = parse_args()

    if not CSV_PATH.exists():
        print(f"[ERROR] CSV not found: {CSV_PATH}")
        sys.exit(1)

    entries = load_csv(CSV_PATH)
    print(f"Loaded {len(entries)} entries with mapped RxCUI from CSV "
          f"(out of 258 total rows; remainder confirmed not in RxNorm)")

    conn = connect_db(args.password)
    try:
        if args.dry_run:
            dry_run_check(conn, entries)
        else:
            apply_updates(conn, entries)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
