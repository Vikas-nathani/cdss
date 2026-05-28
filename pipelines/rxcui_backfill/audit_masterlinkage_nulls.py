#!/usr/bin/env python3
"""
audit_masterlinkage_nulls.py

Counts how many rxcui from drugdb.indian_brand (match_combination='drugbank')
are missing in public.DrugMasterLinkage after resolving through drugdb.ingredients.

Flow:
  indian_brand (match_combination='drugbank') --> rxcui array
      --> drugdb.ingredients (by rxcui) --> unii
          --> DrugMasterLinkage (unii_ids array) --> count nulls

Usage:
    python audit_masterlinkage_nulls.py [--password TEXT]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

_env_path = Path(__file__).parent.parent.parent / ".env"
if _env_path.exists():
    with open(_env_path, encoding="utf-8") as _f:
        import os
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k not in os.environ:
                    os.environ[_k] = _v

from utils import connect_db

AUDIT_SQL = """
WITH brand_rxcui AS (
    -- All unique rxcui values from indian_brand where source is drugbank
    SELECT DISTINCT unnest(rxcui)::text AS rxcui
    FROM drugdb.indian_brand
    WHERE match_combination = 'drugbank'
      AND rxcui IS NOT NULL
      AND array_length(rxcui, 1) > 0
),
rxcui_with_unii AS (
    -- Resolve each rxcui to a unii via drugdb.ingredients
    SELECT DISTINCT br.rxcui, i.unii
    FROM brand_rxcui br
    LEFT JOIN drugdb.ingredients i ON i.rxcui = br.rxcui
),
linkage_uniis AS (
    -- Flatten all unii_ids from DrugMasterLinkage into a lookup set
    SELECT DISTINCT unnest(unii_ids) AS unii
    FROM public."DrugMasterLinkage"
    WHERE unii_ids IS NOT NULL
),
matched AS (
    SELECT
        rcu.rxcui,
        rcu.unii,
        CASE
            WHEN rcu.unii IS NULL      THEN 'no_ingredient'
            WHEN lu.unii IS NOT NULL   THEN 'found_in_linkage'
            ELSE                            'not_in_linkage'
        END AS status
    FROM rxcui_with_unii rcu
    LEFT JOIN linkage_uniis lu ON lu.unii = rcu.unii
)
SELECT
    COUNT(DISTINCT rxcui)                                                   AS total_unique_rxcui,
    COUNT(DISTINCT rxcui) FILTER (WHERE status != 'no_ingredient')          AS rxcui_resolved_to_ingredient,
    COUNT(DISTINCT rxcui) FILTER (WHERE status = 'no_ingredient')           AS rxcui_no_ingredient_match,
    COUNT(DISTINCT unii)  FILTER (WHERE status = 'found_in_linkage')        AS unii_found_in_masterlinkage,
    COUNT(DISTINCT unii)  FILTER (WHERE status = 'not_in_linkage')          AS unii_null_in_masterlinkage
FROM matched;
"""

BRAND_COUNT_SQL = """
SELECT COUNT(*) FROM drugdb.indian_brand WHERE match_combination = 'drugbank';
"""


def run(conn):
    with conn.cursor() as cur:
        cur.execute(BRAND_COUNT_SQL)
        total_brands = cur.fetchone()[0]

        print(f"\n{'='*60}")
        print(f"  DrugMasterLinkage NULL Audit")
        print(f"{'='*60}")
        print(f"  indian_brand rows (match_combination='drugbank'): {total_brands:,}")
        print(f"\n  Running lookup chain... (may take a moment)")

        cur.execute(AUDIT_SQL)
        row = cur.fetchone()
        (
            total_unique_rxcui,
            rxcui_resolved,
            rxcui_no_ingredient,
            unii_found,
            unii_null,
        ) = row

    print(f"\n{'─'*60}")
    print(f"  Step 1 — Unique rxcui from indian_brand:        {total_unique_rxcui:,}")
    print(f"  Step 2 — rxcui matched in drugdb.ingredients:   {rxcui_resolved:,}")
    print(f"           rxcui with no ingredient match:        {rxcui_no_ingredient:,}")
    print(f"  Step 3 — Unique unii found in DrugMasterLinkage:{unii_found:,}")
    print(f"           Unique unii NOT in DrugMasterLinkage:  {unii_null:,}  <-- null count")
    print(f"{'─'*60}\n")


def parse_args():
    p = argparse.ArgumentParser(description="Audit DrugMasterLinkage null coverage")
    p.add_argument("--password", type=str, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    conn = connect_db(args.password)
    try:
        run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
