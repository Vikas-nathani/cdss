#!/usr/bin/env python3
"""
Apply fuzzy matches from fuzzy_match_results.json to the database.

Tiers applied  : 5.1, 5.2, 5.3, 5.4 (all auto — review done manually below)
Tier 5.5       : SKIPPED — broken (single-letter DB entries gave false 100% scores)

Known bad matches explicitly excluded:
  Tier 5.1 : 'Calcitonin (Salmon)'      matched 'Salmon'           (not the drug)
  Tier 5.2 : 'Pegylated Interferon Alpha 2B' matched 'alpha 2a'    (2B ≠ 2A)
  Tier 5.3 : 'Zinc pyrithione'           matched 'Zinc'            (element ≠ compound)
  Tier 5.4 : 'n-acetylcarnosine'         matched 'n-acetyltyrosine' (different molecule)
"""

import json
import os
import sys
import psycopg2

RESULTS_FILE = "fuzzy_match_results.json"
DB_CONFIG    = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432, database="postgres",
                    user="postgres", password=os.environ.get("DB_PASSWORD", ""))

# Ingredients to exclude from each tier (keyed by indian_name)
EXCLUDE = {
    "tier_5_1_parenthetical":     {"Calcitonin (Salmon)"},
    "tier_5_2_high_confidence":   {"Pegylated Interferon Alpha 2B"},
    "tier_5_3_token_based":       {"Zinc pyrithione"},
    "tier_5_4_medium_confidence": {"n-acetylcarnosine"},
    "tier_5_5_partial_match":     None,   # None = skip entire tier
}

TIER_LABELS = {
    "tier_5_1_parenthetical":     "Tier 5.1  Parenthetical extraction",
    "tier_5_2_high_confidence":   "Tier 5.2  High-confidence fuzzy (≥95%)",
    "tier_5_3_token_based":       "Tier 5.3  Token-based exact match",
    "tier_5_4_medium_confidence": "Tier 5.4  Medium-confidence fuzzy (85-94%)",
}

# ── Load results ──────────────────────────────────────────────────────────────
print(f"Loading {RESULTS_FILE} …")
with open(RESULTS_FILE) as fh:
    results = json.load(fh)

# ── Preview what will be applied ──────────────────────────────────────────────
print("\nPLAN — matches to apply:")
sep = "─" * 70
all_to_apply = []

for tier_key, label in TIER_LABELS.items():
    excluded = EXCLUDE.get(tier_key)
    if excluded is None:
        print(f"  {label}: SKIPPED")
        continue

    items = [r for r in results[tier_key] if r["indian_name"] not in excluded]
    skipped = [r for r in results[tier_key] if r["indian_name"] in excluded]
    recs = sum(r["record_count"] for r in items)
    print(f"  {label}: {len(items)} ingredients → {recs:,} records", end="")
    if skipped:
        print(f"  (excluded: {', '.join(r['indian_name'] for r in skipped)})", end="")
    print()
    all_to_apply.extend(items)

total_ingredients = len(all_to_apply)
total_records     = sum(r["record_count"] for r in all_to_apply)
print(f"\n  Total: {total_ingredients} ingredients → ~{total_records:,} records to update")
print(f"  Tier 5.5 partial match: SKIPPED (all 124 matches were false positives)")

print(f"\n{sep}")
print("MATCHES TO BE APPLIED (all tiers):")
print(sep)
for tier_key, label in TIER_LABELS.items():
    excluded = EXCLUDE.get(tier_key)
    if excluded is None:
        continue
    items = [r for r in results[tier_key] if r["indian_name"] not in excluded]
    if not items:
        continue
    print(f"\n  {label}")
    for r in items:
        sim = f"  sim={r['similarity']:.0f}%" if "similarity" in r else ""
        print(f"    '{r['indian_name']}'"
              f"\n       → '{r['matched_with']}'  [{r['drugbank_id']}]{sim}"
              f"  ({r['record_count']:,} records)")

# ── Apply ─────────────────────────────────────────────────────────────────────
print(f"\n{sep}")
ans = input("Apply all updates? (yes/no): ").strip().lower()
if ans != "yes":
    print("Aborted — no changes made.")
    sys.exit(0)

print("\nApplying updates …")
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()

total_rows = 0
for r in all_to_apply:
    cur.execute("""
        UPDATE drugdb.indian_brand_ingredient
           SET drugbank_id = %s
         WHERE ingredient_name_raw = %s
           AND drugbank_id IS NULL
    """, (r["drugbank_id"], r["indian_name"]))
    n = cur.rowcount
    total_rows += n
    sym = "✓" if n > 0 else "✗"
    print(f"  {sym} '{r['indian_name']}' → {r['drugbank_id']} ({n} rows)")

conn.commit()
print(f"\nCommitted. Total rows updated: {total_rows:,}")

# ── Verify ────────────────────────────────────────────────────────────────────
cur.execute("""
    SELECT COUNT(*) AS total, COUNT(drugbank_id) AS with_id
    FROM drugdb.indian_brand_ingredient
""")
total, with_id = cur.fetchone()
pct = round(with_id / total * 100, 2)

print(f"\n{'='*70}")
print("FINAL RESULTS")
print(f"{'='*70}")
print(f"  Before : 559,356 / 580,669 (96.33%)")
print(f"  After  : {with_id:,} / {total:,} ({pct}%)")
print(f"  Gain   : +{with_id - 559_356:,} records")
print(f"  NULL   : {total - with_id:,} records remaining")
print(f"{'='*70}")

cur.close()
conn.close()
print("Done!")
