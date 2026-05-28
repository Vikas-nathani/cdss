#!/usr/bin/env python3
"""
Fuzzy matching script for unmapped drugdb.indian_brand_ingredient records.

Phase 1 (always runs): analysis + dry run, saves fuzzy_match_results.json
Phase 2 (interactive):  applies updates tier-by-tier with prompts
"""

import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

import psycopg2
from fuzzywuzzy import fuzz

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432, database="postgres",
                 user="postgres", password=os.environ.get("DB_PASSWORD", ""))

RESULTS_FILE  = "fuzzy_match_results.json"
LOG_FILE      = "logs/fuzzy_match_update.log"

TIER_HIGH_THRESHOLD   = 95   # Tier 5.2 — auto-update
TIER_MEDIUM_LOW       = 85   # Tier 5.4 — manual review
TIER_PARTIAL_MIN      = 90   # Tier 5.5 — partial/substring

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return s.strip().lower()


def _best_drugbank_id(ingredient_id: str, id_to_dbid: dict) -> str | None:
    return id_to_dbid.get(ingredient_id)


def _make_result(raw, norm, count, tier, matched_name, drugbank_id,
                 method, similarity=None, note=None):
    r = dict(indian_name=raw, indian_norm=norm, record_count=count,
             tier=tier, matched_with=matched_name,
             drugbank_id=drugbank_id, method=method)
    if similarity is not None:
        r["similarity"] = round(similarity, 1)
    if note:
        r["note"] = note
    return r


# ── Tier functions ────────────────────────────────────────────────────────────

def try_parenthetical(raw, norm, count, ingr_by_lower, syn_by_lower, id_to_dbid):
    """
    Tier 5.1 — Extract text inside or before parentheses and try exact match.
    e.g.  "Vitamin B6 (Pyridoxine)"  → try "pyridoxine" then "vitamin b6"
          "Progesterone (Natural Micronized)" → try "progesterone"
    Try before-parenthesis text first (usually the primary drug name), then
    the parenthetical content. Skip parenthetical tokens that are just an
    abbreviation or clearly not a drug name (< 4 chars).
    """
    paren_match = re.search(r'\(([^)]+)\)', raw)
    before_paren = re.sub(r'\s*\([^)]*\)', '', raw).strip()

    candidates = []
    if before_paren:
        candidates.append(before_paren)   # try primary name first
    if paren_match:
        inner = paren_match.group(1).strip()
        if len(inner) >= 4:               # skip abbreviations like "hCG", "DHA"
            candidates.append(inner)

    for candidate in candidates:
        key = _norm(candidate)
        # exact in ingredients
        if key in ingr_by_lower:
            iid, dbid = ingr_by_lower[key]
            return _make_result(raw, norm, count, "5.1", candidate, dbid,
                                "parenthetical → ingredients.name")
        # exact in synonyms
        if key in syn_by_lower:
            iid = syn_by_lower[key][0]
            dbid = _best_drugbank_id(iid, id_to_dbid)
            if dbid:
                return _make_result(raw, norm, count, "5.1", candidate, dbid,
                                    "parenthetical → ingredient_synonyms")
    return None


def try_high_fuzzy(raw, norm, count, ingr_by_lower, syn_by_lower, id_to_dbid):
    """
    Tier 5.2 — Levenshtein ratio ≥ TIER_HIGH_THRESHOLD against all names + synonyms.
    Picks the highest-scoring match; on tie, shortest name wins.
    """
    candidates = []
    key = _norm(raw)

    for name_lower, (iid, dbid) in ingr_by_lower.items():
        score = fuzz.ratio(key, name_lower)
        if score >= TIER_HIGH_THRESHOLD:
            candidates.append((score, len(name_lower), name_lower, dbid, "ingredients.name"))

    for syn_lower, iids in syn_by_lower.items():
        score = fuzz.ratio(key, syn_lower)
        if score >= TIER_HIGH_THRESHOLD:
            dbid = _best_drugbank_id(iids[0], id_to_dbid)
            if dbid:
                candidates.append((score, len(syn_lower), syn_lower, dbid, "ingredient_synonyms"))

    if not candidates:
        return None
    # highest score, then shortest name
    best = max(candidates, key=lambda x: (x[0], -x[1]))

    # Reject if only a trailing letter/digit suffix differs (e.g. "alpha 2b" vs "alpha 2a").
    # Such variants are distinct drugs despite high string similarity.
    raw_tail  = re.sub(r'\s', '', key)[-3:].lower()
    best_tail = re.sub(r'\s', '', best[2])[-3:].lower()
    if raw_tail != best_tail and re.search(r'\d[a-z]$', raw_tail) and re.search(r'\d[a-z]$', best_tail):
        return None

    return _make_result(raw, norm, count, "5.2", best[2], best[3],
                        f"high-confidence fuzzy ({best[4]})", similarity=best[0])


def try_token(raw, norm, count, ingr_by_lower, syn_by_lower, id_to_dbid):
    """
    Tier 5.3 — Split on whitespace/hyphen; try each token as an exact match.
    Tokens shorter than 6 chars are skipped — this prevents matching generic
    element/mineral names like "Zinc", "Iron", "Sodium" which are in DrugBank
    as pure elements but are not the same compound as "Zinc pyrithione", etc.
    """
    tokens = re.split(r'[\s\-/]+', raw.strip())
    tokens = [t for t in tokens if len(t) >= 6]

    for token in tokens:
        key = _norm(token)
        if key in ingr_by_lower:
            iid, dbid = ingr_by_lower[key]
            return _make_result(raw, norm, count, "5.3", token, dbid,
                                f"token '{token}' → ingredients.name")
        if key in syn_by_lower:
            iid = syn_by_lower[key][0]
            dbid = _best_drugbank_id(iid, id_to_dbid)
            if dbid:
                return _make_result(raw, norm, count, "5.3", token, dbid,
                                    f"token '{token}' → ingredient_synonyms")
    return None


def try_medium_fuzzy(raw, norm, count, ingr_by_lower, syn_by_lower, id_to_dbid):
    """
    Tier 5.4 — Levenshtein ratio in [TIER_MEDIUM_LOW, TIER_HIGH_THRESHOLD).
    Flagged for manual review in Phase 2.
    """
    candidates = []
    key = _norm(raw)

    for name_lower, (iid, dbid) in ingr_by_lower.items():
        score = fuzz.ratio(key, name_lower)
        if TIER_MEDIUM_LOW <= score < TIER_HIGH_THRESHOLD:
            candidates.append((score, len(name_lower), name_lower, dbid, "ingredients.name"))

    for syn_lower, iids in syn_by_lower.items():
        score = fuzz.ratio(key, syn_lower)
        if TIER_MEDIUM_LOW <= score < TIER_HIGH_THRESHOLD:
            dbid = _best_drugbank_id(iids[0], id_to_dbid)
            if dbid:
                candidates.append((score, len(syn_lower), syn_lower, dbid, "ingredient_synonyms"))

    if not candidates:
        return None
    best = max(candidates, key=lambda x: (x[0], -x[1]))
    return _make_result(raw, norm, count, "5.4", best[2], best[3],
                        f"medium-confidence fuzzy ({best[4]})", similarity=best[0],
                        note="requires manual review")


def try_partial(raw, norm, count, ingr_by_lower, syn_by_lower, id_to_dbid):
    """
    Tier 5.5 — fuzz.partial_ratio ≥ TIER_PARTIAL_MIN.
    Checks if one string is a near-substring of the other.
    """
    candidates = []
    key = _norm(raw)

    for name_lower, (iid, dbid) in ingr_by_lower.items():
        if len(name_lower) < 6:   # skip single-letter/abbreviation entries
            continue
        score = fuzz.partial_ratio(key, name_lower)
        if score >= TIER_PARTIAL_MIN:
            candidates.append((score, len(name_lower), name_lower, dbid, "ingredients.name"))

    for syn_lower, iids in syn_by_lower.items():
        if len(syn_lower) < 6:
            continue
        score = fuzz.partial_ratio(key, syn_lower)
        if score >= TIER_PARTIAL_MIN:
            dbid = _best_drugbank_id(iids[0], id_to_dbid)
            if dbid:
                candidates.append((score, len(syn_lower), syn_lower, dbid, "ingredient_synonyms"))

    if not candidates:
        return None
    best = max(candidates, key=lambda x: (x[0], -x[1]))
    return _make_result(raw, norm, count, "5.5", best[2], best[3],
                        f"partial/substring match ({best[4]})", similarity=best[0])


# ── Database helpers ──────────────────────────────────────────────────────────

def do_update(cur, ingredient_name_raw, drugbank_id):
    cur.execute("""
        UPDATE drugdb.indian_brand_ingredient
           SET drugbank_id = %s
         WHERE ingredient_name_raw = %s
           AND drugbank_id IS NULL
    """, (drugbank_id, ingredient_name_raw))
    return cur.rowcount


# ── Phase 1 ───────────────────────────────────────────────────────────────────

def run_phase1(conn):
    cur = conn.cursor()

    log.info("Loading unmapped ingredients …")
    cur.execute("""
        SELECT
            ingredient_name_raw,
            ingredient_name_norm,
            COUNT(*) AS record_count
        FROM drugdb.indian_brand_ingredient
        WHERE drugbank_id IS NULL
        GROUP BY ingredient_name_raw, ingredient_name_norm
        ORDER BY COUNT(*) DESC
    """)
    unmapped = cur.fetchall()
    log.info(f"Found {len(unmapped):,} distinct unmapped ingredients")

    log.info("Loading drugdb.ingredients …")
    cur.execute("SELECT id, drugbank_id, name FROM drugdb.ingredients")
    rows = cur.fetchall()
    ingr_by_lower = {_norm(r[2]): (r[0], r[1]) for r in rows}
    id_to_dbid    = {r[0]: r[1] for r in rows}
    log.info(f"  {len(ingr_by_lower):,} ingredient names loaded")

    log.info("Loading drugdb.ingredient_synonyms …")
    cur.execute("SELECT id, synonym FROM drugdb.ingredient_synonyms")
    syn_by_lower: dict[str, list] = defaultdict(list)
    for iid, syn in cur.fetchall():
        syn_by_lower[_norm(syn)].append(iid)
    log.info(f"  {len(syn_by_lower):,} synonyms loaded")

    results = {
        "tier_5_1_parenthetical":    [],
        "tier_5_2_high_confidence":  [],
        "tier_5_3_token_based":      [],
        "tier_5_4_medium_confidence":[],
        "tier_5_5_partial_match":    [],
        "no_match":                  [],
    }

    tiers = [
        ("tier_5_1_parenthetical",    try_parenthetical),
        ("tier_5_2_high_confidence",  try_high_fuzzy),
        ("tier_5_3_token_based",      try_token),
        ("tier_5_4_medium_confidence",try_medium_fuzzy),
        ("tier_5_5_partial_match",    try_partial),
    ]

    log.info("Running matching …")
    for idx, (raw, norm, count) in enumerate(unmapped, 1):
        if idx % 50 == 0:
            log.info(f"  … {idx}/{len(unmapped)}")
        matched = False
        for tier_key, fn in tiers:
            m = fn(raw, norm, count, ingr_by_lower, syn_by_lower, id_to_dbid)
            if m:
                results[tier_key].append(m)
                matched = True
                break
        if not matched:
            results["no_match"].append({"indian_name": raw, "record_count": count})

    cur.close()
    return results, len(unmapped)


def print_phase1_report(results, total_unmapped):
    sep = "=" * 70
    print(f"\n{sep}")
    print("FUZZY MATCHING ANALYSIS RESULTS (DRY RUN — NO DB CHANGES)")
    print(sep)
    print(f"\nDistinct unmapped ingredients analysed : {total_unmapped:,}")

    tier_labels = {
        "tier_5_1_parenthetical":    "Tier 5.1  Parenthetical extraction",
        "tier_5_2_high_confidence":  f"Tier 5.2  High-confidence fuzzy (≥{TIER_HIGH_THRESHOLD}%)",
        "tier_5_3_token_based":      "Tier 5.3  Token-based exact match",
        "tier_5_4_medium_confidence":f"Tier 5.4  Medium-confidence fuzzy ({TIER_MEDIUM_LOW}–{TIER_HIGH_THRESHOLD-1}%, review needed)",
        "tier_5_5_partial_match":    f"Tier 5.5  Partial/substring (≥{TIER_PARTIAL_MIN}%)",
        "no_match":                  "          No match",
    }

    total_records_updatable = 0
    for key, label in tier_labels.items():
        n = len(results[key])
        recs = sum(r["record_count"] for r in results[key])
        if key != "no_match":
            total_records_updatable += recs
        print(f"  {label:<55}  {n:>3} ingredients  {recs:>7,} records")

    current = 559_356
    total   = 580_669
    after   = current + total_records_updatable
    print(f"\n{sep}")
    print(f"POTENTIAL IMPROVEMENT (if all tiers applied):")
    print(f"  Current : {current:,} / {total:,} ({current/total*100:.2f}%)")
    print(f"  After   : {after:,} / {total:,} ({after/total*100:.2f}%)")
    print(sep)

    # Top-10 examples per tier
    for key, label in tier_labels.items():
        if key == "no_match" or not results[key]:
            continue
        print(f"\n{'─'*70}")
        print(f"  {label.strip()} — top examples")
        print(f"{'─'*70}")
        for i, m in enumerate(results[key][:10], 1):
            sim = f"  sim={m['similarity']:.1f}%" if "similarity" in m else ""
            print(f"  {i:>2}. '{m['indian_name']}'"
                  f"\n       → '{m['matched_with']}'  [{m['drugbank_id']}]{sim}"
                  f"  affects {m['record_count']:,} records")

    if results["no_match"]:
        print(f"\n{'─'*70}")
        print(f"  No match ({len(results['no_match'])} ingredients)")
        print(f"{'─'*70}")
        for m in results["no_match"][:20]:
            print(f"    '{m['indian_name']}'  ({m['record_count']:,} records)")
        if len(results["no_match"]) > 20:
            print(f"    … and {len(results['no_match'])-20} more (see {RESULTS_FILE})")


# ── Phase 2 ───────────────────────────────────────────────────────────────────

def run_phase2(conn, results):
    cur = conn.cursor()
    total_updated = 0

    def _apply_tier(tier_key, label, auto=False):
        nonlocal total_updated
        items = results[tier_key]
        if not items:
            return
        recs = sum(r["record_count"] for r in items)
        print(f"\n{'='*70}")
        print(f"{label}")
        print(f"  {len(items)} ingredients → {recs:,} records")

        if not auto:
            ans = input("  Apply updates? (yes/no): ").strip().lower()
            if ans != "yes":
                print("  Skipped.")
                return

        for m in items:
            n = do_update(cur, m["indian_name"], m["drugbank_id"])
            sym = "✓" if n > 0 else "✗"
            print(f"  {sym} '{m['indian_name']}' → {m['drugbank_id']} ({n} rows)")
            total_updated += n

    def _apply_medium():
        nonlocal total_updated
        items = results["tier_5_4_medium_confidence"]
        if not items:
            return
        print(f"\n{'='*70}")
        print(f"Tier 5.4 — Medium-confidence fuzzy ({len(items)} ingredients, review each)")
        print(f"{'='*70}")
        for i, m in enumerate(items, 1):
            sim = m.get("similarity", "?")
            print(f"\n  {i}/{len(items)}  '{m['indian_name']}'")
            print(f"         → '{m['matched_with']}'  [{m['drugbank_id']}]"
                  f"  sim={sim}%  affects {m['record_count']:,} records")
            ans = input("  Accept? (yes/no/quit): ").strip().lower()
            if ans == "quit":
                break
            if ans == "yes":
                n = do_update(cur, m["indian_name"], m["drugbank_id"])
                print(f"  ✓ Updated {n} rows")
                total_updated += n

    _apply_tier("tier_5_1_parenthetical",
                "Tier 5.1 — Parenthetical extraction (high confidence)", auto=False)
    _apply_tier("tier_5_2_high_confidence",
                f"Tier 5.2 — High-confidence fuzzy (≥{TIER_HIGH_THRESHOLD}%)", auto=False)
    _apply_tier("tier_5_3_token_based",
                "Tier 5.3 — Token-based exact match", auto=False)
    _apply_medium()
    _apply_tier("tier_5_5_partial_match",
                f"Tier 5.5 — Partial/substring (≥{TIER_PARTIAL_MIN}%)", auto=False)

    conn.commit()
    log.info(f"Committed. Total rows updated this session: {total_updated:,}")

    # Final verification
    cur.execute("""
        SELECT COUNT(*) AS total, COUNT(drugbank_id) AS with_id
        FROM drugdb.indian_brand_ingredient
    """)
    total, with_id = cur.fetchone()
    pct = round(with_id / total * 100, 2)
    before = 559_356
    print(f"\n{'='*70}")
    print("FINAL RESULTS")
    print(f"{'='*70}")
    print(f"  Before : {before:,} records with drugbank_id")
    print(f"  After  : {with_id:,} records with drugbank_id")
    print(f"  Gain   : +{with_id - before:,}")
    print(f"  Rate   : {pct}%")
    print(f"  NULL   : {total - with_id:,}")
    print(f"{'='*70}")
    cur.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    log.info("Connecting to database …")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        # ── Phase 1 ────────────────────────────────────────────────────────
        results, total_unmapped = run_phase1(conn)

        print_phase1_report(results, total_unmapped)

        with open(RESULTS_FILE, "w") as fh:
            json.dump(results, fh, indent=2)
        log.info(f"Results saved → {RESULTS_FILE}")

        # ── Phase 2 ────────────────────────────────────────────────────────
        print(f"\n{'='*70}")
        ans = input("Proceed to Phase 2 (interactive updates)? (yes/no): ").strip().lower()
        if ans == "yes":
            run_phase2(conn, results)
        else:
            log.info("Phase 2 skipped. Re-run and answer 'yes' to apply updates.")

    except Exception as exc:
        log.exception(f"Fatal error: {exc}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
