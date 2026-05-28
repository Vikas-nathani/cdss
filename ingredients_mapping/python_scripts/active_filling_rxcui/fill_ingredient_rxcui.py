"""
Fill null rxcui values in drugdb.ingredients by name-matching against public.rxnconso.

Strategy:
  - Load all null-rxcui ingredients from DB into memory
  - Build a name -> rxcui map from rxnconso in one batch query, prioritising RXNORM > DRUGBANK > NDDF > others
  - Dry run: log every match, write CSV, print summary — no DB writes
  - Full run: UPDATE ingredients.rxcui + ingredients.rxcui_source, write same log

Usage:
  python fill_ingredient_rxcui.py            # dry run (default)
  python fill_ingredient_rxcui.py --apply    # full run (writes to DB)
"""

import os
import sys
import csv
import logging
import argparse
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
LOGS_DIR = SCRIPT_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

RUN_TAG = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOGS_DIR / f"fill_ingredient_rxcui_{RUN_TAG}.log"
CSV_FILE = LOGS_DIR / f"fill_ingredient_rxcui_{RUN_TAG}.csv"

# SAB priority: lower number = preferred
SAB_PRIORITY = {
    "RXNORM": 1,
    "DRUGBANK": 2,
    "NDDF": 3,
    "MTHSPL": 4,
    "SNOMEDCT_US": 5,
    "ATC": 6,
    "GS": 7,
    "VANDF": 8,
    "MMSL": 9,
    "USP": 10,
}

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_conn():
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:Blumax%24dev@178.236.185.230:5432/postgres",
    ).replace("%24", "$")
    return psycopg2.connect(db_url)


def load_null_rxcui_ingredients(cur):
    """Return list of (id, name, unii, drugbank_id) where rxcui IS NULL."""
    cur.execute("""
        SELECT id, name, unii, drugbank_id
        FROM drugdb.ingredients
        WHERE rxcui IS NULL
        ORDER BY name
    """)
    rows = cur.fetchall()
    log.info(f"Loaded {len(rows):,} ingredients with null rxcui")
    return rows


def build_rxnconso_name_map(cur, names_lower: set[str]) -> dict[str, tuple]:
    """
    For the given set of lowercased names, fetch best (rxcui, sab, str) from rxnconso.
    Returns dict: lower_name -> (rxcui, sab, str_in_rxnconso)
    Uses one bulk query, then picks best SAB per name in Python.
    """
    log.info(f"Querying rxnconso for {len(names_lower):,} distinct names …")

    # Pass names as a temp array — psycopg2 handles the parameterisation
    cur.execute("""
        SELECT LOWER(TRIM(str)) AS name_lower, rxcui, sab, str
        FROM public.rxnconso
        WHERE LOWER(TRIM(str)) = ANY(%s)
    """, (list(names_lower),))

    rows = cur.fetchall()
    log.info(f"rxnconso returned {len(rows):,} candidate rows")

    # Pick best SAB per name
    best: dict[str, tuple] = {}
    for name_lower, rxcui, sab, rxn_str in rows:
        priority = SAB_PRIORITY.get(sab, 99)
        if name_lower not in best or priority < best[name_lower][3]:
            best[name_lower] = (rxcui, sab, rxn_str, priority)

    # Strip priority field before returning
    return {k: (v[0], v[1], v[2]) for k, v in best.items()}


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def run(apply: bool):
    mode = "FULL RUN (will write to DB)" if apply else "DRY RUN (no DB writes)"
    log.info(f"{'='*60}")
    log.info(f"fill_ingredient_rxcui  |  {mode}")
    log.info(f"Log  : {LOG_FILE}")
    log.info(f"CSV  : {CSV_FILE}")
    log.info(f"{'='*60}")

    conn = get_conn()
    cur = conn.cursor()

    # Step 1: load null-rxcui ingredients
    ingredients = load_null_rxcui_ingredients(cur)

    # Step 2: build name lookup set
    names_lower = {row[1].strip().lower() for row in ingredients}

    # Step 3: get best rxcui per name from rxnconso
    name_map = build_rxnconso_name_map(cur, names_lower)

    # Step 4: match and collect updates
    updates = []       # (rxcui, ingredient_id)
    csv_rows = []      # for log CSV

    no_match = []

    for ing_id, ing_name, ing_unii, ing_drugbank_id in ingredients:
        key = ing_name.strip().lower()
        if key in name_map:
            rxcui, sab, rxn_str = name_map[key]
            updates.append((rxcui, str(ing_id)))
            csv_rows.append({
                "ingredient_id":      str(ing_id),
                "ingredient_name":    ing_name,
                "rxnconso_str":       rxn_str,
                "matched_sab":        sab,
                "rxcui_assigned":     rxcui,
                "unii":               ing_unii or "",
                "drugbank_id":        ing_drugbank_id or "",
            })
        else:
            no_match.append(ing_name)

    log.info(f"Matched  : {len(updates):,} ingredients")
    log.info(f"No match : {len(no_match):,} ingredients")

    # Step 5: write CSV log
    if csv_rows:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_rows[0].keys())
            writer.writeheader()
            writer.writerows(csv_rows)
        log.info(f"CSV written: {CSV_FILE}")

    # Step 6: apply updates if --apply
    if apply and updates:
        log.info("Applying UPDATE to drugdb.ingredients …")
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE drugdb.ingredients
            SET rxcui        = %s,
                rxcui_source = 'rxnconso_name_match',
                created_by   = 'rxnconso_fill',
                updated_at   = now()
            WHERE id = %s::uuid
              AND rxcui IS NULL
            """,
            updates,
            page_size=500,
        )
        conn.commit()
        log.info(f"Committed {cur.rowcount:,} rows updated")
    elif apply and not updates:
        log.info("Nothing to update.")
    else:
        log.info("Dry run complete — no changes written to DB.")

    cur.close()
    conn.close()

    log.info("Done.")
    print(f"\nSummary:")
    print(f"  Matched (fillable) : {len(updates):,}")
    print(f"  No match           : {len(no_match):,}")
    print(f"  Log file           : {LOG_FILE}")
    print(f"  CSV file           : {CSV_FILE}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill null rxcui in drugdb.ingredients from rxnconso")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to DB (default: dry run only)",
    )
    args = parser.parse_args()

    # Load .env if present
    env_file = SCRIPT_DIR / ".env"
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)

    run(apply=args.apply)
