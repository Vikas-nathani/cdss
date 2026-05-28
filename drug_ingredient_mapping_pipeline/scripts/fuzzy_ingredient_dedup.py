#!/usr/bin/env python3
"""
Fuzzy deduplication of drugdb.ingredients skeleton rows (rxcui set, drugbank_id NULL)
against existing DrugBank rows (drugbank_id NOT NULL).

Phase 1: Dry-run analysis — no DB writes.
Phase 2: Apply Level 1/2/3 matches (UPDATE rxcui on DrugBank row, DELETE skeleton row).
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

try:
    from fuzzywuzzy import fuzz
except ImportError:
    print("ERROR: fuzzywuzzy not installed. Run: pip install fuzzywuzzy python-Levenshtein")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR   = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

JSON_OUT   = os.path.join(LOGS_DIR, "fuzzy_ingredient_match_results.json")
APPLY_LOG  = os.path.join(LOGS_DIR, "fuzzy_ingredient_match_apply_log.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def connect(args):
    return psycopg2.connect(
        host=args.host, port=args.port,
        dbname=args.dbname, user=args.user, password=args.password,
    )


def load_skeletons(conn):
    """SET A: rows with rxcui but no drugbank_id."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, name, rxcui
            FROM drugdb.ingredients
            WHERE rxcui IS NOT NULL
              AND drugbank_id IS NULL
        """)
        rows = cur.fetchall()
    log.info(f"  Skeleton rows (SET A): {len(rows):,}")
    return [dict(r) for r in rows]


def load_drugbank(conn):
    """SET B: rows with drugbank_id."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, name, rxcui, drugbank_id
            FROM drugdb.ingredients
            WHERE drugbank_id IS NOT NULL
        """)
        rows = cur.fetchall()
    log.info(f"  DrugBank rows (SET B): {len(rows):,}")
    return [dict(r) for r in rows]


def load_synonyms(conn):
    """{ synonym_lower: drugbank_ingredient_id } — only for rows with drugbank_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.id, s.synonym
            FROM drugdb.ingredient_synonyms s
            JOIN drugdb.ingredients i ON i.id = s.id
            WHERE i.drugbank_id IS NOT NULL
              AND s.synonym IS NOT NULL
        """)
        rows = cur.fetchall()
    mapping = {}
    for ing_id, synonym in rows:
        key = synonym.lower().strip()
        if key not in mapping:
            mapping[key] = str(ing_id)
    log.info(f"  Synonyms loaded: {len(mapping):,}")
    return mapping


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

# Thresholds
LEVEL3_MIN = 90
LEVEL4_MIN = 75
LEVEL5_MIN = 60


def match_all(skeletons, drugbank_rows, synonym_map):
    """
    Returns a list of result dicts — one per skeleton row.
    """
    # Build fast lookup dicts
    db_by_name = {}   # lower_name -> list of drugbank rows (handle rare dupes)
    for row in drugbank_rows:
        key = row["name"].lower().strip()
        db_by_name.setdefault(key, []).append(row)

    db_by_id = {str(r["id"]): r for r in drugbank_rows}

    results = []
    for skel in skeletons:
        skel_key = skel["name"].lower().strip()
        result = _match_one(skel, skel_key, db_by_name, db_by_id, synonym_map, drugbank_rows)
        results.append(result)

    return results


def _match_one(skel, skel_key, db_by_name, db_by_id, synonym_map, drugbank_rows):
    """Run levels 1→5, return first match found."""

    base = {
        "skeleton_id":   str(skel["id"]),
        "skeleton_name": skel["name"],
        "skeleton_rxcui": skel["rxcui"],
    }

    # --- Level 1: exact name match ---
    if skel_key in db_by_name:
        target = db_by_name[skel_key][0]
        return {**base, "level": 1, "category": "EXACT_MATCH", "score": 100,
                "drugbank_id": target["drugbank_id"],
                "target_id":   str(target["id"]),
                "target_name": target["name"],
                "target_rxcui": target.get("rxcui")}

    # --- Level 2: synonym match ---
    if skel_key in synonym_map:
        target_id = synonym_map[skel_key]
        target = db_by_id.get(target_id)
        if target:
            return {**base, "level": 2, "category": "SYNONYM_MATCH", "score": 100,
                    "drugbank_id": target["drugbank_id"],
                    "target_id":   str(target["id"]),
                    "target_name": target["name"],
                    "target_rxcui": target.get("rxcui")}

    # --- Levels 3/4/5: fuzzy ---
    best_score = 0
    best_target = None
    for row in drugbank_rows:
        score = fuzz.token_sort_ratio(skel_key, row["name"].lower().strip())
        if score > best_score:
            best_score = score
            best_target = row

    if best_score >= LEVEL3_MIN:
        cat = "HIGH_CONFIDENCE"
        lvl = 3
    elif best_score >= LEVEL4_MIN:
        cat = "MEDIUM_CONFIDENCE"
        lvl = 4
    elif best_score >= LEVEL5_MIN:
        cat = "LOW_CONFIDENCE"
        lvl = 5
    else:
        return {**base, "level": 0, "category": "NO_MATCH", "score": best_score,
                "drugbank_id": None, "target_id": None, "target_name": None, "target_rxcui": None}

    return {**base, "level": lvl, "category": cat, "score": best_score,
            "drugbank_id": best_target["drugbank_id"],
            "target_id":   str(best_target["id"]),
            "target_name": best_target["name"],
            "target_rxcui": best_target.get("rxcui")}


# ---------------------------------------------------------------------------
# Phase 1 report
# ---------------------------------------------------------------------------

def print_report(results):
    cats = {
        "EXACT_MATCH":       [],
        "SYNONYM_MATCH":     [],
        "HIGH_CONFIDENCE":   [],
        "MEDIUM_CONFIDENCE": [],
        "LOW_CONFIDENCE":    [],
        "NO_MATCH":          [],
    }
    for r in results:
        cats[r["category"]].append(r)

    W = 80
    print("\n" + "=" * W)
    print("FUZZY INGREDIENT DEDUPLICATION ANALYSIS")
    print("=" * W)
    print(f"Skeleton rows analyzed:        {len(results)}")
    print(f"Level 1 - Exact match:         {len(cats['EXACT_MATCH'])}")
    print(f"Level 2 - Synonym match:       {len(cats['SYNONYM_MATCH'])}")
    print(f"Level 3 - High confidence:     {len(cats['HIGH_CONFIDENCE'])}  (score >= 90, auto-appliable)")
    print(f"Level 4 - Medium confidence:   {len(cats['MEDIUM_CONFIDENCE'])}  (score 75-89, needs review)")
    print(f"Level 5 - Low confidence:      {len(cats['LOW_CONFIDENCE'])}  (score 60-74, informational)")
    print(f"No match found:                {len(cats['NO_MATCH'])}")
    print("=" * W)

    # Levels 1+2+3 combined header
    auto_apply = cats["EXACT_MATCH"] + cats["SYNONYM_MATCH"] + cats["HIGH_CONFIDENCE"]
    if auto_apply:
        print(f"\nLEVEL 1+2+3 MATCHES — will be AUTO-APPLIED in Phase 2 ({len(auto_apply)} rows):")
        _print_match_table(auto_apply)

    if cats["MEDIUM_CONFIDENCE"]:
        print(f"\nLEVEL 4 — MEDIUM CONFIDENCE (needs your review, will NOT be auto-applied):")
        _print_match_table(cats["MEDIUM_CONFIDENCE"])

    if cats["LOW_CONFIDENCE"]:
        print(f"\nLEVEL 5 — LOW CONFIDENCE (informational only):")
        _print_match_table(cats["LOW_CONFIDENCE"])

    if cats["NO_MATCH"]:
        print(f"\nNO MATCH — these skeleton rows will remain unchanged ({len(cats['NO_MATCH'])}):")
        for r in cats["NO_MATCH"]:
            print(f"  {r['skeleton_name']!r:45s}  rxcui={r['skeleton_rxcui']}")

    print("=" * W)
    return cats


def _print_match_table(rows):
    hdr = f"  {'skeleton_name':<40} {'drugbank_name':<40} {'score':>5}  {'drugbank_id':<12}  rxcui"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for r in rows:
        rxcui_clash = " ⚠ target has rxcui" if r.get("target_rxcui") else ""
        print(
            f"  {str(r['skeleton_name']):<40} "
            f"{str(r['target_name']):<40} "
            f"{r['score']:>5}  "
            f"{str(r['drugbank_id']):<12}  "
            f"{r['skeleton_rxcui']}"
            f"{rxcui_clash}"
        )


# ---------------------------------------------------------------------------
# Phase 2 — apply (fills drugbank_id on skeleton rows only, no deletions)
# ---------------------------------------------------------------------------

# Skeleton names confirmed as false positives — never apply these
EXCLUSIONS = {
    "sodium nitrite",     # matched Sodium nitrate — different compound
    "aminolevulinate",    # matched Hexaminolevulinate — different compound
}

def apply_matches(conn, cats, apply_log_path):
    to_apply = cats["EXACT_MATCH"] + cats["SYNONYM_MATCH"] + cats["HIGH_CONFIDENCE"]
    if not to_apply:
        print("Nothing to apply.")
        return

    excluded = [r for r in to_apply if r["skeleton_name"].lower() in EXCLUSIONS]
    skipped  = [r for r in to_apply if r["skeleton_name"].lower() not in EXCLUSIONS
                                    and r.get("target_rxcui")]
    safe     = [r for r in to_apply if r["skeleton_name"].lower() not in EXCLUSIONS
                                    and not r.get("target_rxcui")]

    print(f"\nPhase 2: filling drugbank_id on {len(safe)} skeleton rows.")
    print(f"  Excluded (false positives): {len(excluded)}")
    print(f"  Skipped (target has rxcui, unrelated filter): {len(skipped)}")
    print()

    applied   = 0
    errors    = []
    log_lines = [
        "fuzzy_ingredient_dedup.py — Phase 2 apply log",
        f"Run: {datetime.now().isoformat()}",
        f"Mode: SET drugbank_id on skeleton rows only — no deletions",
        f"Safe to apply: {len(safe)} | Excluded: {len(excluded)} | Skipped: {len(skipped)}",
        "=" * 80,
    ]

    with conn:  # single transaction — full rollback on any error
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for r in safe:
                try:
                    # Fetch before state
                    cur.execute(
                        "SELECT id, name, rxcui, drugbank_id FROM drugdb.ingredients WHERE id = %s",
                        (r["skeleton_id"],)
                    )
                    before = dict(cur.fetchone())

                    # Apply: set drugbank_id on the skeleton row
                    cur.execute("""
                        UPDATE drugdb.ingredients
                        SET drugbank_id = %s,
                            updated_at  = NOW()
                        WHERE id = %s
                          AND drugbank_id IS NULL
                    """, (r["drugbank_id"], r["skeleton_id"]))
                    rowcount = cur.rowcount

                    # Fetch after state
                    cur.execute(
                        "SELECT id, name, rxcui, drugbank_id FROM drugdb.ingredients WHERE id = %s",
                        (r["skeleton_id"],)
                    )
                    after = dict(cur.fetchone())

                    if rowcount == 1:
                        applied += 1
                        print(f"  UPDATED: '{before['name']}'")
                        print(f"    BEFORE: rxcui={before['rxcui']}  drugbank_id={before['drugbank_id']}")
                        print(f"    AFTER : rxcui={after['rxcui']}   drugbank_id={after['drugbank_id']}")
                        print()
                        log_lines.append(f"UPDATED: {before['name']!r}")
                        log_lines.append(f"  BEFORE: rxcui={before['rxcui']}  drugbank_id={before['drugbank_id']}")
                        log_lines.append(f"  AFTER : rxcui={after['rxcui']}   drugbank_id={after['drugbank_id']}")
                    else:
                        msg = f"WARNING: 0 rows updated for '{r['skeleton_name']}' (drugbank_id may already be set)"
                        print(msg)
                        log_lines.append(msg)
                        errors.append(msg)

                except Exception as e:
                    err = f"ERROR on '{r['skeleton_name']}': {e}"
                    errors.append(err)
                    print(err)
                    log_lines.append(err)
                    raise  # triggers full rollback

    # Excluded report
    if excluded:
        log_lines.append("\nEXCLUDED (false positives — not applied):")
        for r in excluded:
            msg = f"  EXCLUDED: '{r['skeleton_name']}' → '{r['target_name']}' (score={r['score']})"
            print(msg)
            log_lines.append(msg)

    # Skipped report
    if skipped:
        log_lines.append("\nSKIPPED (target already had rxcui — unrelated safety filter):")
        for r in skipped:
            msg = f"  SKIPPED: '{r['skeleton_name']}' → '{r['target_name']}'"
            log_lines.append(msg)

    log_lines += [
        "=" * 80,
        f"Skeleton rows updated (drugbank_id filled) : {applied}",
        f"Excluded (false positives)                 : {len(excluded)}",
        f"Skipped                                    : {len(skipped)}",
        f"Errors                                     : {len(errors)}",
        "NOTE: No rows were deleted.",
    ]

    with open(apply_log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines) + "\n")

    print(f"{'='*60}")
    print(f"Phase 2 complete.")
    print(f"  Skeleton rows updated (drugbank_id set) : {applied}")
    print(f"  Excluded (false positives)              : {len(excluded)}")
    print(f"  Skipped                                 : {len(skipped)}")
    print(f"  Errors                                  : {len(errors)}")
    print(f"  Deletions                               : 0 (none)")
    print(f"  Log saved → {apply_log_path}")
    print(f"  Errors                       : {len(errors)}")
    print(f"  Log saved → {apply_log_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fuzzy dedup of drugdb.ingredients skeleton rows")
    parser.add_argument("--host",     default=os.environ.get("DB_HOST", "localhost"))
    parser.add_argument("--port",     default=5432, type=int)
    parser.add_argument("--dbname",   default="postgres")
    parser.add_argument("--user",     default="postgres")
    parser.add_argument("--password", required=True, help="DB password")
    parser.add_argument("--phase1-only",   action="store_true", help="Only run Phase 1, do not prompt for Phase 2")
    parser.add_argument("--skip-confirm",  action="store_true", help="Skip confirmation prompt and run Phase 2 automatically")
    args = parser.parse_args()

    # ── Phase 1 ──────────────────────────────────────────────────────────────
    log.info("Connecting to database ...")
    conn = connect(args)
    log.info("  Connected.")

    log.info("Loading data ...")
    skeletons     = load_skeletons(conn)
    drugbank_rows = load_drugbank(conn)
    synonym_map   = load_synonyms(conn)

    if not skeletons:
        print("No skeleton rows found (rxcui NOT NULL AND drugbank_id IS NULL). Nothing to do.")
        conn.close()
        return

    log.info(f"Running matching across {len(skeletons)} skeleton rows × {len(drugbank_rows):,} DrugBank rows ...")
    results = match_all(skeletons, drugbank_rows, synonym_map)

    cats = print_report(results)

    # Save JSON
    output = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total":             len(results),
            "exact_match":       len(cats["EXACT_MATCH"]),
            "synonym_match":     len(cats["SYNONYM_MATCH"]),
            "high_confidence":   len(cats["HIGH_CONFIDENCE"]),
            "medium_confidence": len(cats["MEDIUM_CONFIDENCE"]),
            "low_confidence":    len(cats["LOW_CONFIDENCE"]),
            "no_match":          len(cats["NO_MATCH"]),
        },
        "results": results,
    }
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nFull results saved → {JSON_OUT}")

    # ── Phase 2 ──────────────────────────────────────────────────────────────
    auto_count = len(cats["EXACT_MATCH"]) + len(cats["SYNONYM_MATCH"]) + len(cats["HIGH_CONFIDENCE"])

    if args.phase1_only or auto_count == 0:
        if auto_count == 0:
            print("No Level 1/2/3 matches to apply.")
        conn.close()
        return

    if not args.skip_confirm:
        print(f"\n{auto_count} matches are ready to apply (Level 1+2+3).")
        print("This will UPDATE rxcui on matching DrugBank rows and DELETE the skeleton rows.")
        answer = input("Proceed with Phase 2? [yes/no]: ").strip().lower()
        if answer != "yes":
            print("Phase 2 skipped.")
            conn.close()
            return

    apply_matches(conn, cats, APPLY_LOG)
    conn.close()


if __name__ == "__main__":
    main()
