#!/usr/bin/env python3
"""
Find and fix the 35 ingredient_name_raw values that were matched in the log
but still have NULL drugbank_id in drugdb.indian_brand_ingredient.
"""

import os
import re
import sys
from collections import Counter
import psycopg2

LOG_FILE = "logs/indian_brand_drugbank.log"
DB_CONFIG = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432, database="postgres",
                 user="postgres", password=os.environ.get("DB_PASSWORD", ""))

# ── 1. Parse log ──────────────────────────────────────────────────────────────
print(f"Reading log file: {LOG_FILE}")
pattern = re.compile(r"Tier \d match: raw='([^']+)'.*?→ drugbank_id=(\w+)")
# ingredient_name_raw → Counter of drugbank_ids seen
vote: dict[str, Counter] = {}

with open(LOG_FILE, "r") as fh:
    for line in fh:
        m = pattern.search(line)
        if m:
            raw, dbid = m.group(1), m.group(2)
            vote.setdefault(raw, Counter())[dbid] += 1

# Pick most-common drugbank_id per ingredient
log_matches: dict[str, str] = {raw: ctr.most_common(1)[0][0] for raw, ctr in vote.items()}
print(f"Found {len(log_matches):,} unique ingredient matches in log")

# ── 2. Query DB for still-NULL records ────────────────────────────────────────
print("\nQuerying database for NULL records …")
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()

cur.execute("""
    SELECT ingredient_name_raw, COUNT(*) AS cnt
    FROM drugdb.indian_brand_ingredient
    WHERE drugbank_id IS NULL
    GROUP BY ingredient_name_raw
    ORDER BY cnt DESC
""")
null_rows = cur.fetchall()   # [(ingredient_name_raw, cnt), ...]
null_dict = {row[0]: row[1] for row in null_rows}
print(f"Found {len(null_dict):,} distinct ingredients still NULL")

# ── 3. Find intersection ──────────────────────────────────────────────────────
# Exact match
exact_missing = {raw: (log_matches[raw], null_dict[raw])
                 for raw in log_matches if raw in null_dict}

# Fuzzy: compare stripped/lower versions to catch whitespace differences
lower_log   = {raw.strip().lower(): (raw, dbid) for raw, dbid in log_matches.items()}
lower_null  = {raw.strip().lower(): (raw, cnt)  for raw, cnt  in null_dict.items()}

fuzzy_missing = {}
for key in lower_log:
    if key in lower_null and lower_null[key][0] not in exact_missing:
        log_raw,  dbid = lower_log[key]
        null_raw, cnt  = lower_null[key]
        fuzzy_missing[null_raw] = (dbid, cnt, log_raw)  # db_raw → (dbid, cnt, log_raw)

total_missing = len(exact_missing) + len(fuzzy_missing)
print(f"\nIdentifying missing matches …")
print(f"  Exact-name matches still NULL : {len(exact_missing)}")
print(f"  Fuzzy (whitespace) matches    : {len(fuzzy_missing)}")
print(f"  Total missing                 : {total_missing}")

if total_missing == 0:
    print("\nNothing to fix — all matched ingredients already have drugbank_id.")
    cur.close(); conn.close(); sys.exit(0)

# ── 4. Show and update ────────────────────────────────────────────────────────
print("\nMissing Ingredients:")
print(f"{'Ingredient':<45} {'DrugBank ID':<14} Affects")
print("-" * 70)

updates = []  # [(db_raw, drugbank_id, cnt)]

for raw, (dbid, cnt) in sorted(exact_missing.items(), key=lambda x: -x[1][1]):
    print(f"  '{raw}' → {dbid} ({cnt} records)  [exact]")
    updates.append((raw, dbid, cnt))

for db_raw, (dbid, cnt, log_raw) in sorted(fuzzy_missing.items(), key=lambda x: -x[1][1]):
    print(f"  '{db_raw}' → {dbid} ({cnt} records)  [fuzzy ← log:'{log_raw}']")
    updates.append((db_raw, dbid, cnt))

print(f"\nUpdating {len(updates)} missing ingredients …")
before_total = 559_356  # from previous run

total_rows = 0
for db_raw, dbid, expected_cnt in updates:
    cur.execute("""
        UPDATE drugdb.indian_brand_ingredient
           SET drugbank_id = %s
         WHERE ingredient_name_raw = %s
           AND drugbank_id IS NULL
    """, (dbid, db_raw))
    n = cur.rowcount
    total_rows += n
    status = "✓" if n > 0 else "✗"
    print(f"  {status} Updated '{db_raw}' → {dbid} ({n} records)")

conn.commit()
print(f"\nTotal records updated: {total_rows:,}")

# ── 5. Verify ─────────────────────────────────────────────────────────────────
print("\nFINAL VERIFICATION:")
cur.execute("""
    SELECT COUNT(*) AS total, COUNT(drugbank_id) AS with_id
    FROM drugdb.indian_brand_ingredient
""")
total, with_id = cur.fetchone()
after = with_id
pct = round(after / total * 100, 2)
print(f"  Before: {before_total:,} records with drugbank_id")
print(f"  After:  {after:,} records with drugbank_id")
print(f"  Diff:   +{after - before_total:,}")
print(f"  Rate:   {pct}%")

cur.close()
conn.close()
print("\nDone!")
