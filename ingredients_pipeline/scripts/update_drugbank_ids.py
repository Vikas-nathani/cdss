#!/usr/bin/env python3
"""
Update drugdb.ingredients.drugbank_id for 190 skeleton rows
from drugbank_lookup_results_COMPLETE.csv.

Safety rules:
- Only updates rows WHERE drugbank_id IS NULL (never overwrites existing data)
- Runs inside a single transaction; rolls back if >5 errors
- Verification query runs after commit
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     5432,
    "dbname":   "postgres",
    "user":     "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
    "connect_timeout": 15,
}

CSV_PATH = "/home/nathanivikas890_gmail_com/cdss/data/drugbank_lookup_results_COMPLETE.csv"

def main():
    # ── Load CSV ──────────────────────────────────────────────────────────────
    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df)} rows from CSV")

    # Only rows that have a non-empty drugbank_id
    to_update = df[df["drugbank_id"].notna() & (df["drugbank_id"].astype(str).str.strip() != "")]
    skipped   = len(df) - len(to_update)
    print(f"  → {len(to_update)} rows have a drugbank_id to write")
    print(f"  → {skipped} rows skipped (empty drugbank_id in CSV)\n")

    # ── Connect ───────────────────────────────────────────────────────────────
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        print(f"ERROR connecting to DB: {e}")
        sys.exit(1)

    conn.autocommit = False
    cur = conn.cursor()

    updated        = 0
    zero_match     = 0
    errors         = 0
    error_limit    = 5
    all_rxcuis     = []

    print("Running UPDATEs inside transaction…")
    print("-" * 60)

    try:
        for _, row in to_update.iterrows():
            rxcui       = str(row["rxcui"]).strip()
            drugbank_id = str(row["drugbank_id"]).strip()
            confidence  = str(row.get("confidence", "")).strip()
            all_rxcuis.append(rxcui)

            try:
                cur.execute(
                    """
                    UPDATE drugdb.ingredients
                       SET drugbank_id = %s,
                           updated_at  = NOW()
                     WHERE rxcui       = %s
                       AND drugbank_id IS NULL
                    """,
                    (drugbank_id, rxcui),
                )
                rows_affected = cur.rowcount
                if rows_affected > 0:
                    updated += rows_affected
                    print(f"  UPDATED rxcui={rxcui:>10} → {drugbank_id}  [{confidence}]")
                else:
                    zero_match += 1
                    print(f"  NO MATCH rxcui={rxcui:>10} (already filled or not found)")

            except psycopg2.Error as e:
                errors += 1
                print(f"  ERROR rxcui={rxcui}: {e}")
                if errors > error_limit:
                    print(f"\n  {errors} errors exceeded limit of {error_limit} — ROLLING BACK")
                    conn.rollback()
                    cur.close()
                    conn.close()
                    sys.exit(1)

        # ── Commit ────────────────────────────────────────────────────────────
        conn.commit()
        print("\nTransaction COMMITTED successfully.")

    except Exception as e:
        conn.rollback()
        print(f"\nUnexpected error — ROLLED BACK: {e}")
        cur.close()
        conn.close()
        sys.exit(1)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n=== UPDATE SUMMARY ===")
    print(f"Total CSV rows        : {len(df)}")
    print(f"Rows with drugbank_id : {len(to_update)}")
    print(f"Successfully updated  : {updated}")
    print(f"Zero-match (skipped)  : {zero_match}")
    print(f"Skipped (empty CSV)   : {skipped}")
    print(f"Errors                : {errors}")

    # ── Verification query ────────────────────────────────────────────────────
    print("\n=== VERIFICATION ===")
    rxcui_list = tuple(all_rxcuis)
    cur.execute(
        """
        SELECT
            CASE WHEN i.drugbank_id IS NOT NULL THEN 'FILLED' ELSE 'STILL_NULL' END AS status,
            COUNT(*) AS count
        FROM drugdb.ingredients i
        WHERE i.rxcui = ANY(%s)
        GROUP BY 1
        ORDER BY 1
        """,
        (list(rxcui_list),),
    )
    rows = cur.fetchall()
    for status, count in rows:
        print(f"  {status:<12}: {count}")

    # Also show a breakdown of which confidence levels made it in
    print("\n=== CONFIDENCE BREAKDOWN IN DB (updated rows) ===")
    cur.execute(
        """
        SELECT i.drugbank_id, i.rxcui
        FROM drugdb.ingredients i
        WHERE i.rxcui = ANY(%s)
          AND i.drugbank_id IS NOT NULL
        """,
        (list(rxcui_list),),
    )
    db_rows = {str(r[1]): r[0] for r in cur.fetchall()}
    conf_counts = {"GREEN": 0, "YELLOW": 0, "OTHER": 0}
    for _, row in to_update.iterrows():
        rxcui = str(row["rxcui"]).strip()
        if rxcui in db_rows:
            c = str(row.get("confidence", "")).strip()
            if c in conf_counts:
                conf_counts[c] += 1
            else:
                conf_counts["OTHER"] += 1
    for k, v in conf_counts.items():
        if v:
            print(f"  {k:<8}: {v}")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
