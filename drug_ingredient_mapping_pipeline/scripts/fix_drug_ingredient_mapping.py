#!/usr/bin/env python3
"""
Fix missing rows in drugdb.drug_ingredient_mapping.

Strategy: extract ing_rxcui from DrugMasterLinkage JSONB,
join with drugdb.drug and drugdb.ingredients, insert only missing pairs.
"""

import os
import sys
import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host":            os.environ.get("DB_HOST", "localhost"),
    "port":            5432,
    "dbname":          "postgres",
    "user":            "postgres",
    "password":        os.environ.get("DB_PASSWORD", ""),
    "connect_timeout": 15,
}

BATCH_SIZE = 500


# ── helpers ───────────────────────────────────────────────────────────────────

def log(msg):
    print(msg, flush=True)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    log("Connecting to database…")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        log(f"ERROR: cannot connect — {e}")
        sys.exit(1)

    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── Step 1: baseline count ────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) AS cnt FROM drugdb.drug_ingredient_mapping")
    baseline = cur.fetchone()["cnt"]
    log(f"Baseline rows in drug_ingredient_mapping: {baseline:,}")

    # ── Step 2: build ingredient rxcui lookup (rxcui → uuid) ─────────────────
    log("Loading drugdb.ingredients rxcui → id map…")
    cur.execute("""
        SELECT id, rxcui
        FROM drugdb.ingredients
        WHERE rxcui IS NOT NULL AND rxcui <> ''
    """)
    ing_by_rxcui = {}
    for row in cur.fetchall():
        rxcui = str(row["rxcui"]).strip()
        if rxcui:
            ing_by_rxcui.setdefault(rxcui, row["id"])  # keep first if duplicates
    log(f"  → {len(ing_by_rxcui):,} ingredients with rxcui")

    # ── Step 3: build existing mapping set (formulation_id, ingredient_id) ───
    log("Loading existing drug_ingredient_mapping pairs…")
    cur.execute("""
        SELECT formulation_id::text, ingredient_id::text
        FROM drugdb.drug_ingredient_mapping
    """)
    existing = set()
    for row in cur.fetchall():
        existing.add((str(row["formulation_id"]), str(row["ingredient_id"])))
    log(f"  → {len(existing):,} existing pairs loaded")

    # ── Step 4: extract candidate rows from JSONB ─────────────────────────────
    log("Extracting candidates from DrugMasterLinkage JSONB…")
    cur.execute("""
        SELECT
            d.formulation_id,
            d.rxcui         AS drug_rxcui,
            ing.value ->> 'ing_rxcui'                      AS ing_rxcui,
            ing.value #>> '{scdc,mass}'                     AS mass_raw,
            ing.value #>> '{scdc,unit}'                     AS unit
        FROM public."DrugMasterLinkage" dml
        JOIN drugdb.drug d
            ON d.master_linkage_id = dml.master_linkage_id
        JOIN LATERAL jsonb_array_elements(
            dml.combined_clean_jsonb -> 'rxnorm'
        ) rx(value) ON TRUE
        JOIN LATERAL jsonb_array_elements(
            rx.value -> 'ingredients'
        ) ing(value) ON TRUE
        WHERE d.rxcui = rx.value ->> 'rxcui'
          AND ing.value ->> 'ing_rxcui' IS NOT NULL
    """)
    candidates = cur.fetchall()
    log(f"  → {len(candidates):,} candidate (formulation, ingredient) pairs from JSONB")

    # ── Step 5: filter to only missing rows, build insert list ───────────────
    log("Filtering to missing rows…")

    to_insert    = []
    skip_reasons = {}

    def skip(reason):
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1

    for row in candidates:
        formulation_id = str(row["formulation_id"])
        ing_rxcui      = (row["ing_rxcui"] or "").strip()
        mass_raw       = row["mass_raw"]
        unit           = row["unit"]

        if not ing_rxcui:
            skip("ing_rxcui is NULL/empty")
            continue

        ingredient_id = ing_by_rxcui.get(ing_rxcui)
        if ingredient_id is None:
            skip(f"ing_rxcui={ing_rxcui} not in drugdb.ingredients")
            continue

        ingredient_id_str = str(ingredient_id)
        if (formulation_id, ingredient_id_str) in existing:
            skip("pair already exists")
            continue

        # cast mass
        mass = None
        if mass_raw is not None:
            try:
                mass = float(mass_raw)
            except (ValueError, TypeError):
                pass  # insert NULL mass, keep going

        to_insert.append((formulation_id, ingredient_id, mass, unit))
        existing.add((formulation_id, ingredient_id_str))  # prevent dupes within this run

    log(f"  → {len(to_insert):,} new rows to insert")
    log(f"  → skip breakdown:")
    for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
        log(f"       {count:>6,}  {reason}")

    if not to_insert:
        log("\nNothing to insert. Exiting.")
        _print_validation(cur)
        cur.close()
        conn.close()
        return

    # ── Step 6: batch insert ──────────────────────────────────────────────────
    log(f"\nInserting in batches of {BATCH_SIZE}…")

    total_attempted = 0
    total_inserted  = 0
    total_errored   = 0

    batches = [to_insert[i:i+BATCH_SIZE] for i in range(0, len(to_insert), BATCH_SIZE)]

    for batch_num, batch in enumerate(batches, 1):
        total_attempted += len(batch)
        try:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO drugdb.drug_ingredient_mapping
                    (formulation_id, ingredient_id, mass, unit)
                VALUES %s
                ON CONFLICT (formulation_id, ingredient_id) DO NOTHING
                """,
                batch,
                template="(%s::uuid, %s::uuid, %s::numeric, %s)",
                page_size=BATCH_SIZE,
            )
            inserted = cur.rowcount if cur.rowcount >= 0 else len(batch)
            conn.commit()
            total_inserted += inserted
            log(f"  Batch {batch_num:>4}/{len(batches)} — inserted {inserted:>4} rows"
                f"  (running total: {total_inserted:,})")
        except psycopg2.Error as e:
            conn.rollback()
            total_errored += len(batch)
            log(f"  Batch {batch_num:>4}/{len(batches)} — ERROR (rolled back): {e}")

    # ── Step 7: summary + validation ─────────────────────────────────────────
    log("\n=== INSERT SUMMARY ===")
    log(f"Total rows attempted : {total_attempted:,}")
    log(f"Total inserted       : {total_inserted:,}")
    log(f"Total errored/skipped: {total_errored:,}")
    log(f"Pre-existing skipped : {skip_reasons.get('pair already exists', 0):,}")

    _print_validation(cur)

    cur.close()
    conn.close()


def _print_validation(cur):
    log("\n=== VALIDATION ===")
    cur.execute("""
        SELECT
            100271            AS expected,
            COUNT(*)          AS actual,
            100271 - COUNT(*) AS still_missing
        FROM drugdb.drug_ingredient_mapping
    """)
    row = cur.fetchone()
    log(f"  Expected     : {int(row['expected']):,}")
    log(f"  Actual       : {int(row['actual']):,}")
    log(f"  Still missing: {int(row['still_missing']):,}")


if __name__ == "__main__":
    main()
