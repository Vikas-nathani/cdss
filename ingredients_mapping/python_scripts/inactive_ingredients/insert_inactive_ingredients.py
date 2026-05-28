"""
Extract unique inactive ingredients from DrugMasterLinkage.combined_clean_jsonb
(dailymed -> drug_info -> products[*] -> inactive_ingredients[*]) and insert
those not already in drugdb.ingredients with:
  - type        = 'inactive'
  - unii        from rxnsat (FDA_UNII_CODE) via rxnconso name match
  - drugbank_id from DrugSourceMaster (drugbank) name match
  - rxcui       from rxnconso name match (best SAB priority)

Usage:
  python3 insert_inactive_ingredients.py            # dry run (default)
  python3 insert_inactive_ingredients.py --apply    # write to DB
"""

import os
import sys
import csv
import argparse
import logging
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
LOGS_DIR = SCRIPT_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

RUN_TAG = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOGS_DIR / f"insert_inactive_ingredients_{RUN_TAG}.log"
CSV_FILE = LOGS_DIR / f"insert_inactive_ingredients_{RUN_TAG}.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# rxnconso SAB priority — lower = preferred
SAB_PRIORITY = {
    "RXNORM": 1, "DRUGBANK": 2, "NDDF": 3, "MTHSPL": 4,
    "SNOMEDCT_US": 5, "ATC": 6, "GS": 7, "VANDF": 8, "MMSL": 9, "USP": 10,
}

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def get_conn():
    load_dotenv(SCRIPT_DIR / ".env")
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:Blumax%24dev@178.236.185.230:5432/postgres",
    ).replace("%24", "$")
    return psycopg2.connect(db_url)

# ---------------------------------------------------------------------------
# Step 1: unique inactive names from DailyMed
# ---------------------------------------------------------------------------

def fetch_inactive_names(cur) -> set[str]:
    log.info("Fetching unique inactive ingredient names from DailyMed ...")
    cur.execute("""
        SELECT DISTINCT LOWER(TRIM(ing_name)) AS name
        FROM public."DrugMasterLinkage",
             jsonb_array_elements(
               CASE WHEN combined_clean_jsonb ? 'dailymed'
                         AND jsonb_typeof(combined_clean_jsonb -> 'dailymed' -> 'drug_info' -> 'products') = 'array'
                    THEN combined_clean_jsonb -> 'dailymed' -> 'drug_info' -> 'products'
                    ELSE '[]'::jsonb END
             ) AS prod,
             jsonb_array_elements_text(
               CASE WHEN jsonb_typeof(prod -> 'inactive_ingredients') = 'array'
                    THEN prod -> 'inactive_ingredients'
                    ELSE '[]'::jsonb END
             ) AS ing_name
        WHERE TRIM(ing_name) <> ''
    """)
    names = {row[0] for row in cur.fetchall()}
    log.info(f"  {len(names):,} unique inactive names found")
    return names

# ---------------------------------------------------------------------------
# Step 2: names already in ingredients table
# ---------------------------------------------------------------------------

def fetch_existing_names(cur) -> set[str]:
    log.info("Loading existing ingredient names ...")
    cur.execute("SELECT LOWER(TRIM(name)) FROM drugdb.ingredients")
    names = {row[0] for row in cur.fetchall()}
    log.info(f"  {len(names):,} existing ingredients")
    return names

# ---------------------------------------------------------------------------
# Step 3: UNII lookup — rxnconso MTHSPL code field contains the FDA UNII
#          MTHSPL (Metathesaurus from SPL) uses UNII as the source code,
#          giving near-complete coverage for excipients/inactive ingredients.
#          Fallback: rxnsat FDA_UNII_CODE (DrugBank-sourced, ~10k entries).
# ---------------------------------------------------------------------------

def fetch_unii_map(cur, names: set[str]) -> dict[str, str]:
    """Returns lower_name -> unii."""
    log.info("Looking up UNII codes via rxnconso MTHSPL code field ...")
    cur.execute("""
        SELECT DISTINCT ON (LOWER(TRIM(str)))
               LOWER(TRIM(str)) AS name_lower,
               code             AS unii
        FROM public.rxnconso
        WHERE sab  = 'MTHSPL'
          AND code != 'NOCODE'
          AND code IS NOT NULL
          AND LOWER(TRIM(str)) = ANY(%s)
        ORDER BY LOWER(TRIM(str))
    """, (list(names),))
    unii_map: dict[str, str] = {row[0]: row[1] for row in cur.fetchall()}
    log.info(f"  UNII found (MTHSPL) for {len(unii_map):,} / {len(names):,} names")

    # Fallback: rxnsat FDA_UNII_CODE for any still missing
    missing = names - set(unii_map)
    if missing:
        cur.execute("""
            SELECT LOWER(TRIM(c.str)) AS name_lower, s.atv AS unii
            FROM public.rxnconso c
            JOIN public.rxnsat s ON s.rxcui = c.rxcui AND s.atn = 'FDA_UNII_CODE'
            WHERE LOWER(TRIM(c.str)) = ANY(%s)
              AND s.atv IS NOT NULL AND TRIM(s.atv) <> ''
        """, (list(missing),))
        for name_lower, unii in cur.fetchall():
            if name_lower not in unii_map:
                unii_map[name_lower] = unii
        log.info(f"  UNII found (rxnsat fallback) total: {len(unii_map):,} / {len(names):,} names")

    return unii_map

# ---------------------------------------------------------------------------
# Step 4: DrugBank ID lookup — first try name match, then UNII cross-reference
#          Many excipients exist in DrugBank; UNII is the reliable bridge key.
# ---------------------------------------------------------------------------

def fetch_drugbank_id_map(cur, names: set[str], unii_map: dict[str, str]) -> dict[str, str]:
    """Returns lower_name -> drugbank_id."""
    log.info("Looking up DrugBank IDs via DrugSourceMaster (name + UNII) ...")

    # Pass 1: name match
    cur.execute("""
        SELECT LOWER(TRIM(standardized_records -> 'drug_info' ->> 'name')) AS name_lower,
               standardized_records -> 'drug_info' ->> 'drugbank_id'       AS drugbank_id
        FROM public."DrugSourceMaster"
        WHERE source = 'drugbank'
          AND LOWER(TRIM(standardized_records -> 'drug_info' ->> 'name')) = ANY(%s)
          AND (standardized_records -> 'drug_info' ->> 'drugbank_id') IS NOT NULL
    """, (list(names),))
    db_map: dict[str, str] = {}
    for name_lower, drugbank_id in cur.fetchall():
        if name_lower not in db_map:
            db_map[name_lower] = drugbank_id
    log.info(f"  DrugBank ID found (name match): {len(db_map):,} / {len(names):,} names")

    # Pass 2: UNII cross-reference for remaining names
    missing_names = names - set(db_map)
    unii_to_name  = {v: k for k, v in unii_map.items() if k in missing_names}
    if unii_to_name:
        cur.execute("""
            SELECT standardized_records -> 'drug_info' ->> 'unii'       AS unii,
                   standardized_records -> 'drug_info' ->> 'drugbank_id' AS drugbank_id
            FROM public."DrugSourceMaster"
            WHERE source = 'drugbank'
              AND standardized_records -> 'drug_info' ->> 'unii' = ANY(%s)
              AND (standardized_records -> 'drug_info' ->> 'drugbank_id') IS NOT NULL
        """, (list(unii_to_name.keys()),))
        for unii, drugbank_id in cur.fetchall():
            name_lower = unii_to_name.get(unii)
            if name_lower and name_lower not in db_map:
                db_map[name_lower] = drugbank_id
        log.info(f"  DrugBank ID found (UNII match) total: {len(db_map):,} / {len(names):,} names")

    return db_map

# ---------------------------------------------------------------------------
# Step 5: RxCUI lookup — rxnconso name match with SAB priority
# ---------------------------------------------------------------------------

def fetch_rxcui_map(cur, names: set[str]) -> dict[str, tuple[str, str]]:
    """Returns lower_name -> (rxcui, sab)."""
    log.info("Looking up RxCUI codes via rxnconso ...")
    cur.execute("""
        SELECT LOWER(TRIM(str)) AS name_lower, rxcui, sab
        FROM public.rxnconso
        WHERE LOWER(TRIM(str)) = ANY(%s)
    """, (list(names),))
    best: dict[str, tuple] = {}
    for name_lower, rxcui, sab in cur.fetchall():
        priority = SAB_PRIORITY.get(sab, 99)
        if name_lower not in best or priority < best[name_lower][2]:
            best[name_lower] = (rxcui, sab, priority)
    rxcui_map = {k: (v[0], v[1]) for k, v in best.items()}
    log.info(f"  RxCUI found for {len(rxcui_map):,} / {len(names):,} names")
    return rxcui_map

# ---------------------------------------------------------------------------
# Step 6: insert
# ---------------------------------------------------------------------------

INSERT_SQL = """
INSERT INTO drugdb.ingredients
    (name, type, unii, drugbank_id, rxcui, rxcui_source, created_by, created_at, updated_at)
VALUES %s
ON CONFLICT DO NOTHING
"""

def insert_rows(cur, rows: list[tuple], dry_run: bool) -> int:
    if dry_run:
        log.info(f"  DRY RUN — would insert {len(rows):,} rows")
        return len(rows)
    psycopg2.extras.execute_values(
        cur,
        INSERT_SQL,
        rows,
        template="(%s, 'inactive'::drugdb.ingredient_type, %s, %s, %s, %s, %s, %s, %s)",
        page_size=500,
    )
    return len(rows)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(apply: bool):
    mode = "FULL RUN (writing to DB)" if apply else "DRY RUN (no DB writes)"
    log.info("=" * 60)
    log.info(f"insert_inactive_ingredients  |  {mode}")
    log.info(f"Log : {LOG_FILE}")
    log.info(f"CSV : {CSV_FILE}")
    log.info("=" * 60)

    conn = get_conn()
    cur = conn.cursor()

    inactive_names = fetch_inactive_names(cur)
    existing_names = fetch_existing_names(cur)

    new_names      = inactive_names - existing_names
    already_exists = inactive_names & existing_names

    log.info(f"Already in ingredients table : {len(already_exists):,}")
    log.info(f"New to insert                : {len(new_names):,}")

    if not new_names:
        log.info("Nothing new to insert.")
        cur.close()
        conn.close()
        return

    # Enrich new names with UNII, DrugBank ID, RxCUI
    unii_map      = fetch_unii_map(cur, new_names)
    drugbank_map  = fetch_drugbank_id_map(cur, new_names, unii_map)
    rxcui_map     = fetch_rxcui_map(cur, new_names)

    now = datetime.utcnow()
    rows = []
    csv_rows = []

    for name in sorted(new_names):
        unii        = unii_map.get(name)
        drugbank_id = drugbank_map.get(name)
        rxcui_val, rxcui_src = rxcui_map.get(name, (None, None))
        rxcui_source = f"rxnconso_{rxcui_src}" if rxcui_src else None

        rows.append((name, unii, drugbank_id, rxcui_val, rxcui_source, "dailymed_inactive_fill", now, now))
        csv_rows.append({
            "name":        name,
            "unii":        unii or "",
            "drugbank_id": drugbank_id or "",
            "rxcui":       rxcui_val or "",
            "rxcui_source": rxcui_source or "",
        })

    # Write CSV
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_rows[0].keys())
        writer.writeheader()
        writer.writerows(csv_rows)
    log.info(f"CSV written: {CSV_FILE}")

    # Summary of enrichment coverage
    filled_unii      = sum(1 for r in csv_rows if r["unii"])
    filled_drugbank  = sum(1 for r in csv_rows if r["drugbank_id"])
    filled_rxcui     = sum(1 for r in csv_rows if r["rxcui"])
    log.info(f"Enrichment coverage out of {len(new_names):,} new rows:")
    log.info(f"  UNII        : {filled_unii:,}")
    log.info(f"  DrugBank ID : {filled_drugbank:,}")
    log.info(f"  RxCUI       : {filled_rxcui:,}")

    inserted = insert_rows(cur, rows, dry_run=not apply)

    if apply:
        conn.commit()
        log.info(f"Committed {inserted:,} new inactive ingredients.")
    else:
        log.info("Dry run complete — no changes written.")

    cur.close()
    conn.close()

    print(f"\nSummary:")
    print(f"  Total unique inactive (DailyMed) : {len(inactive_names):,}")
    print(f"  Already in ingredients table     : {len(already_exists):,}")
    print(f"  {'Inserted' if apply else 'Would insert'}                  : {inserted:,}")
    print(f"    - with UNII        : {filled_unii:,}")
    print(f"    - with DrugBank ID : {filled_drugbank:,}")
    print(f"    - with RxCUI       : {filled_rxcui:,}")
    print(f"  Log : {LOG_FILE}")
    print(f"  CSV : {CSV_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert DailyMed inactive ingredients into drugdb.ingredients"
    )
    parser.add_argument("--apply", action="store_true", help="Write to DB (default: dry run)")
    args = parser.parse_args()
    run(apply=args.apply)
