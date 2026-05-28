#!/usr/bin/env python3
"""
Second-pass population of drugdb.drug_ingredient_mapping using ingredient_synonyms as fallback.
Run with --dry-run first, then --execute for full insertion.
"""

import json
import logging
import os
import sys
import argparse
from datetime import datetime

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

DB = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432, dbname="postgres", user="postgres", password=os.environ.get("DB_PASSWORD", ""))

LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE  = os.path.join(LOGS_DIR, "drug_ingredient_mapping_second_pass.log")
JSON_FILE = os.path.join(LOGS_DIR, "drug_ingredient_mapping_second_pass.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def load_direct_lookup(conn):
    """{ lower(name): ingredient_id }"""
    log.info("Loading direct lookup from drugdb.ingredients ...")
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM drugdb.ingredients WHERE name IS NOT NULL")
        rows = cur.fetchall()
    lookup = {name.lower(): ing_id for ing_id, name in rows}
    log.info(f"  direct_lookup: {len(lookup):,} entries")
    return lookup


def load_synonym_lookup(conn, direct_lookup):
    """{ lower(synonym): ingredient_id } — excludes names already in direct_lookup."""
    log.info("Loading synonym lookup from drugdb.ingredient_synonyms ...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.id, s.synonym
            FROM drugdb.ingredient_synonyms s
            JOIN drugdb.ingredients i ON i.id = s.id
            WHERE i.drugbank_id IS NOT NULL
              AND s.synonym IS NOT NULL
        """)
        rows = cur.fetchall()

    lookup = {}
    for ing_id, synonym in rows:
        key = synonym.lower()
        if key not in direct_lookup:
            lookup[key] = ing_id

    log.info(f"  synonym_lookup: {len(lookup):,} entries")
    return lookup


def load_ingredient_name_map(conn):
    """{ id: name }"""
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM drugdb.ingredients")
        return {row[0]: row[1] for row in cur.fetchall()}


def load_ingredient_drugbank_id_map(conn):
    """{ id: drugbank_id }"""
    with conn.cursor() as cur:
        cur.execute("SELECT id, drugbank_id FROM drugdb.ingredients")
        return {row[0]: row[1] for row in cur.fetchall()}


def load_already_mapped(conn):
    """Set of (formulation_id, ingredient_id) already in the target table."""
    log.info("Loading existing mappings from drugdb.drug_ingredient_mapping ...")
    with conn.cursor() as cur:
        cur.execute("SELECT formulation_id, ingredient_id FROM drugdb.drug_ingredient_mapping")
        rows = cur.fetchall()
    s = set(rows)
    log.info(f"  already mapped: {len(s):,} rows")
    return s


# ---------------------------------------------------------------------------
# Extraction query
# ---------------------------------------------------------------------------

EXTRACT_SQL = """
WITH rxnorm_ingredients AS (
    SELECT
        dml.master_linkage_id,
        rxnorm_entry->>'rxcui'                          AS rxcui,
        ingredient_entry->>'name'                       AS ingredient_name,
        (ingredient_entry->'scdc'->>'mass')::NUMERIC    AS mass,
        ingredient_entry->'scdc'->>'unit'               AS unit
    FROM public."DrugMasterLinkage" dml,
         jsonb_array_elements(dml.combined_clean_jsonb->'rxnorm') AS rxnorm_entry,
         jsonb_array_elements(rxnorm_entry->'ingredients')        AS ingredient_entry
    WHERE ingredient_entry->>'name' IS NOT NULL
)
SELECT
    ri.ingredient_name,
    ri.mass,
    ri.unit,
    d.formulation_id
FROM rxnorm_ingredients ri
JOIN drugdb.drug d
    ON d.master_linkage_id = ri.master_linkage_id
   AND d.rxcui = ri.rxcui
"""

INSERT_SQL = """
INSERT INTO drugdb.drug_ingredient_mapping (formulation_id, ingredient_id, mass, unit)
VALUES %s
ON CONFLICT (formulation_id, ingredient_id) DO NOTHING
"""


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def run(dry_run: bool):
    mode = "DRY RUN" if dry_run else "FULL EXECUTION"
    log.info("=" * 60)
    log.info(f"Second-pass ingredient mapping — {mode}")
    log.info("=" * 60)

    # Separate read/write connections so batch commits don't invalidate the server-side cursor
    read_conn  = psycopg2.connect(**DB)
    write_conn = psycopg2.connect(**DB)
    read_conn.autocommit  = False
    write_conn.autocommit = False

    try:
        direct_lookup    = load_direct_lookup(read_conn)
        synonym_lookup   = load_synonym_lookup(read_conn, direct_lookup)
        already_mapped   = load_already_mapped(read_conn)
        ing_name_map     = load_ingredient_name_map(read_conn)
        ing_drugbank_map = load_ingredient_drugbank_id_map(read_conn)

        # counters
        total_processed = 0
        skipped_direct  = 0
        skipped_already = 0
        new_rows        = 0
        unrecoverable   = 0

        recovered_names     = {}  # lower(rxnorm_name) -> ingredient_id
        unrecoverable_names = {}  # rxnorm_name -> count
        sample_matches      = []  # up to 20 distinct names

        batch         = []
        BATCH_SIZE    = 1000
        PROGRESS_STEP = 5000

        log.info("Fetching rows via server-side cursor ...")

        with read_conn.cursor("second_pass_cur", cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.itersize = 2000
            cur.execute(EXTRACT_SQL)

            for row in cur:
                total_processed += 1
                ing_name       = row["ingredient_name"]
                mass           = row["mass"]
                unit           = row["unit"]
                formulation_id = row["formulation_id"]

                key = ing_name.lower()

                # Already covered by first pass
                if key in direct_lookup:
                    skipped_direct += 1
                    continue

                if key in synonym_lookup:
                    ingredient_id = synonym_lookup[key]
                    pair = (formulation_id, ingredient_id)

                    if pair in already_mapped:
                        skipped_already += 1
                        continue

                    recovered_names[key] = ingredient_id
                    already_mapped.add(pair)
                    new_rows += 1
                    batch.append((formulation_id, ingredient_id, mass, unit))

                    if len(sample_matches) < 20 and key not in {m["rxnorm_name"].lower() for m in sample_matches}:
                        sample_matches.append({
                            "rxnorm_name":    ing_name,
                            "matched_synonym": next(
                                (s for s, i in synonym_lookup.items() if i == ingredient_id), "?"
                            ),
                            "drugbank_name":  ing_name_map.get(ingredient_id, "?"),
                            "drugbank_id":    ing_drugbank_map.get(ingredient_id, "?"),
                        })

                    if not dry_run and len(batch) >= BATCH_SIZE:
                        psycopg2.extras.execute_values(
                            write_conn.cursor(), INSERT_SQL, batch, page_size=BATCH_SIZE
                        )
                        write_conn.commit()
                        batch.clear()

                else:
                    unrecoverable += 1
                    unrecoverable_names[ing_name] = unrecoverable_names.get(ing_name, 0) + 1

                if total_processed % PROGRESS_STEP == 0:
                    log.info(
                        f"  Processed {total_processed:,} | "
                        f"new={new_rows:,} | skipped_direct={skipped_direct:,} | "
                        f"unrecoverable={unrecoverable:,}"
                    )

        # Flush remaining batch
        if not dry_run and batch:
            psycopg2.extras.execute_values(
                write_conn.cursor(), INSERT_SQL, batch, page_size=BATCH_SIZE
            )
            write_conn.commit()
            batch.clear()

        # Build stats
        top10_unrecoverable = sorted(unrecoverable_names.items(), key=lambda x: -x[1])[:10]

        stats = {
            "mode": mode,
            "timestamp": datetime.now().isoformat(),
            "total_rows_processed":            total_processed,
            "new_rows_inserted":               new_rows if not dry_run else 0,
            "new_rows_would_insert":           new_rows if dry_run else None,
            "rows_skipped_direct_match":       skipped_direct,
            "rows_skipped_already_mapped":     skipped_already,
            "rows_unrecoverable":              unrecoverable,
            "distinct_ingredient_names_recovered":     len(recovered_names),
            "distinct_ingredient_names_unrecoverable": [n for n, _ in top10_unrecoverable],
            "top10_unrecoverable_with_counts":         top10_unrecoverable,
            "sample_matches": sample_matches,
        }

        log.info("")
        log.info("=" * 60)
        log.info(f"RESULTS ({mode})")
        log.info("=" * 60)
        log.info(f"  Total rows processed           : {total_processed:,}")
        log.info(f"  Skipped (direct name match)    : {skipped_direct:,}")
        log.info(f"  Skipped (already in mapping)   : {skipped_already:,}")
        log.info(f"  New rows {'inserted' if not dry_run else 'would be inserted'}        : {new_rows:,}")
        log.info(f"  Unrecoverable rows             : {unrecoverable:,}")
        log.info(f"  Distinct ingredients recovered : {len(recovered_names):,}")
        log.info("")
        log.info("Top 10 unrecoverable ingredient names:")
        for name, cnt in top10_unrecoverable:
            log.info(f"    {cnt:6,}x  {name}")
        log.info("")
        log.info("Sample synonym matches (up to 20):")
        for m in sample_matches:
            log.info(f"    '{m['rxnorm_name']}' -> '{m['drugbank_name']}' ({m['drugbank_id']})")

        # Verification (full run only)
        if not dry_run:
            log.info("")
            log.info("=" * 60)
            log.info("VERIFICATION QUERIES")
            log.info("=" * 60)
            with write_conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM drugdb.drug_ingredient_mapping")
                total_count = cur.fetchone()[0]
                log.info(f"  Total rows in mapping table : {total_count:,}")

                cur.execute("""
                    SELECT
                        COUNT(DISTINCT formulation_id)                              AS covered,
                        (SELECT COUNT(*) FROM drugdb.drug)                          AS total,
                        ROUND(COUNT(DISTINCT formulation_id) * 100.0 /
                              (SELECT COUNT(*) FROM drugdb.drug), 2)                AS pct
                    FROM drugdb.drug_ingredient_mapping
                """)
                cov = cur.fetchone()
                log.info(f"  Formulations covered        : {cov[0]:,} / {cov[1]:,} = {cov[2]}%")

                cur.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT formulation_id, ingredient_id
                        FROM drugdb.drug_ingredient_mapping
                        GROUP BY formulation_id, ingredient_id
                        HAVING COUNT(*) > 1
                    ) dupes
                """)
                dupes = cur.fetchone()[0]
                log.info(f"  Duplicate rows              : {dupes} (expect 0)")

            stats["verification"] = {
                "total_mapping_rows":   total_count,
                "formulations_covered": cov[0],
                "total_formulations":   cov[1],
                "coverage_pct":         float(cov[2]),
                "duplicate_rows":       dupes,
            }

        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, default=str)
        log.info("")
        log.info(f"Stats saved -> {JSON_FILE}")
        log.info(f"Log   saved -> {LOG_FILE}")

        return stats

    finally:
        read_conn.close()
        write_conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Second-pass drug_ingredient_mapping population")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing to DB")
    parser.add_argument("--execute", action="store_true", help="Perform full execution (insert rows)")
    args = parser.parse_args()

    if args.execute:
        run(dry_run=False)
    else:
        run(dry_run=True)
        print("")
        print("=" * 60)
        print("DRY RUN COMPLETE. Review results above.")
        print("To execute: python3 second_pass_ingredient_mapping.py --execute")
        print("=" * 60)
