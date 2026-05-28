#!/usr/bin/env python3
"""
Bulk update drugbank_id in drugdb.indian_brand_ingredient using match data
parsed from the Phase 1 log file. Uses a temp table + single UPDATE JOIN
instead of row-by-row, completing in minutes rather than hours.
"""

import os
import re
import sys
import psycopg2
from psycopg2.extras import execute_values

LOG_FILE = "logs/indian_brand_drugbank.log"
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
}

# ── Step 1: Parse log file ────────────────────────────────────────────────────
print("Step 1: Parsing log file …")
pattern = re.compile(r"Tier \d match: raw='([^']+)'.*?→ drugbank_id=(\w+)")
seen = {}  # raw_name → drugbank_id  (dedup; first occurrence wins)

with open(LOG_FILE, "r") as fh:
    for line in fh:
        m = pattern.search(line)
        if m:
            raw_name, drugbank_id = m.group(1), m.group(2)
            if raw_name not in seen:
                seen[raw_name] = drugbank_id

matches = list(seen.items())  # [(raw_name, drugbank_id), ...]
print(f"  Extracted {len(matches):,} distinct matches from log")

if not matches:
    print("ERROR: No matches found — check log path and format.")
    sys.exit(1)

# ── Step 2-8: Database work ───────────────────────────────────────────────────
conn = None
try:
    print("\nStep 2: Connecting to database …")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()
    print("  Connected.")

    print("\nStep 3: Creating temp table …")
    cur.execute("""
        CREATE TEMP TABLE temp_drugbank_matches (
            ingredient_name_raw VARCHAR,
            drugbank_id         VARCHAR
        ) ON COMMIT DROP
    """)

    print("\nStep 4: Bulk-inserting matches into temp table …")
    execute_values(
        cur,
        "INSERT INTO temp_drugbank_matches (ingredient_name_raw, drugbank_id) VALUES %s",
        matches,
        page_size=10_000,
    )
    print(f"  Inserted {len(matches):,} rows.")

    print("\nStep 5: Creating index on temp table …")
    cur.execute("CREATE INDEX idx_tmp_raw ON temp_drugbank_matches(ingredient_name_raw)")
    print("  Index created.")

    print("\nStep 6: Executing single bulk UPDATE …")
    cur.execute("""
        UPDATE drugdb.indian_brand_ingredient AS target
           SET drugbank_id = matches.drugbank_id
          FROM temp_drugbank_matches AS matches
         WHERE LOWER(target.ingredient_name_raw) = LOWER(matches.ingredient_name_raw)
           AND target.drugbank_id IS NULL
    """)
    rows_updated = cur.rowcount
    print(f"  Updated {rows_updated:,} records.")

    print("\nStep 7: Committing …")
    conn.commit()
    print("  Committed.")

    print("\nStep 8: Verification query …")
    cur.execute("""
        SELECT
            COUNT(*)                                                    AS total,
            COUNT(drugbank_id)                                          AS with_id,
            ROUND(COUNT(drugbank_id)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS pct
        FROM drugdb.indian_brand_ingredient
    """)
    total, with_id, pct = cur.fetchone()
    print(f"  Final stats: {with_id:,}/{total:,} records have drugbank_id ({pct}%)")

    cur.close()
    print("\nDone!")

except Exception as exc:
    print(f"\nERROR: {exc}")
    if conn:
        conn.rollback()
        print("Rolled back.")
    sys.exit(1)

finally:
    if conn:
        conn.close()
