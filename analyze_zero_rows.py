#!/usr/bin/env python3
"""Analyze ZERO_ROWS failed drugs using JSONL cache + DB text."""

import csv
import json
import os
import re
from pathlib import Path

import psycopg2

DB = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432, dbname="postgres", user="postgres", password=os.environ.get("DB_PASSWORD", ""))

JSONL_FILES = [
    Path("~/cdss/logs/dosing_regimen_responses.jsonl").expanduser(),
    Path("~/cdss/logs/retry_deferred_responses.jsonl").expanduser(),
]
OUTPUT_CSV = Path("~/cdss/zero_rows_analysis.csv").expanduser()

DOSE_NUMBER_RE = re.compile(
    r'(\d+\s*(mg|mcg|ml|kg|units?|iu)|(mg|mcg|ml|kg|units?|iu)\s*\d+)',
    re.IGNORECASE
)
DOSE_KEYWORDS = re.compile(
    r'\b(daily|twice|tablet|capsule|dose|dosing|administer|infusion|injection|'
    r'oral|intravenous|weekly|hourly|morning|bedtime|titrate)\b',
    re.IGNORECASE
)

FETCH_SQL = """
SELECT f.master_linkage_id, f.generic_name,
    dml.combined_clean_jsonb -> 'openfda'
        -> 'labeling_content'
        -> 'dosage_and_administration'
        ->> 'text' AS openfda_text,
    dml.combined_clean_jsonb -> 'dailymed'
        -> 'labeling_content'
        -> 'dosage_and_administration'
        ->> 'content' AS dailymed_text
FROM drugdb.failed_drugs f
JOIN public."DrugMasterLinkage" dml ON dml.master_linkage_id = f.master_linkage_id
WHERE f.failure_reason = 'ZERO_ROWS'
"""


def load_jsonl_cache():
    cache = {}
    for path in JSONL_FILES:
        if not path.exists():
            print(f"  WARNING: {path} not found, skipping")
            continue
        count = 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                mid = entry.get("master_linkage_id")
                if not mid:
                    continue
                ts = entry.get("timestamp", "")
                existing = cache.get(mid)
                if existing is None or ts > existing.get("timestamp", ""):
                    cache[mid] = entry
                count += 1
        print(f"  Loaded {count:,} entries from {path.name}")
    return cache


def classify(dosage_text):
    length = len(dosage_text)
    if length < 100:
        return "GENUINELY_EMPTY", False, False, True
    has_nums = bool(DOSE_NUMBER_RE.search(dosage_text))
    has_kw = bool(DOSE_KEYWORDS.search(dosage_text))
    if has_nums and has_kw:
        cat = "BAD_LLM_RESPONSE"
    elif has_kw:
        cat = "AMBIGUOUS"
    else:
        cat = "GENUINELY_EMPTY"
    return cat, has_nums, has_kw, False


def main():
    print("=== ZERO_ROWS Analysis ===\n")

    # STEP 1
    print("STEP 1 — Loading JSONL cache...")
    cache = load_jsonl_cache()
    print(f"  Total unique drugs in cache: {len(cache):,}\n")

    # STEP 2
    print("STEP 2 — Querying DB for ZERO_ROWS drugs...")
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute(FETCH_SQL)
        rows = cur.fetchall()
    conn.close()
    print(f"  Found {len(rows):,} ZERO_ROWS drugs\n")

    # STEP 3-4
    print("STEP 3-4 — Classifying drugs...\n")
    results = []
    in_cache = 0

    for master_linkage_id, generic_name, openfda_text, dailymed_text in rows:
        parts = []
        if openfda_text:
            parts.append(openfda_text.strip())
        if dailymed_text:
            parts.append(dailymed_text.strip())
        dosage_text = "\n\n".join(parts)

        cat, has_nums, has_kw, too_short = classify(dosage_text)

        cached = cache.get(master_linkage_id)
        if cached:
            in_cache += 1
            llm_raw = cached.get("raw_response") or cached.get("response") or json.dumps(cached)[:500]
        else:
            llm_raw = "NOT_IN_CACHE"

        results.append({
            "master_linkage_id": master_linkage_id,
            "generic_name": generic_name or "",
            "category": cat,
            "dosage_text_length": len(dosage_text),
            "has_dose_numbers": has_nums,
            "has_dose_keywords": has_kw,
            "text_too_short": too_short,
            "dosage_text_sample": dosage_text[:300],
            "llm_raw_response": str(llm_raw)[:1000],
        })

    # STEP 5
    print("STEP 5 — Writing CSV...")
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"  Written to {OUTPUT_CSV}\n")

    # STEP 6
    from collections import Counter
    cats = Counter(r["category"] for r in results)

    print("=== SUMMARY ===")
    print(f"Total ZERO_ROWS drugs:  {len(results)}")
    print(f"Found in JSONL cache:   {in_cache}")
    print(f"Not in cache:           {len(results) - in_cache}")
    print()
    print("Category breakdown:")
    print(f"  BAD_LLM_RESPONSE:   {cats['BAD_LLM_RESPONSE']:>4}  (retry — LLM failed on good text)")
    print(f"  AMBIGUOUS:          {cats['AMBIGUOUS']:>4}  (retry once — may or may not work)")
    print(f"  GENUINELY_EMPTY:    {cats['GENUINELY_EMPTY']:>4}  (skip — mark NO_DOSING_INFO permanently)")


if __name__ == "__main__":
    main()
