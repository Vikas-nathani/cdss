#!/usr/bin/env python3
"""
Fix empty generic_name in drugdb.drug by pulling from DrugMasterLinkage JSON.

Source: combined_clean_jsonb -> 'dailymed' -> 'drug_info' -> 'products' -> 0 -> 'generic_name'
Target: drugdb.drug.generic_name where it is blank
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = int(os.getenv("PG_PORT", 5432))
DB_NAME = os.getenv("PG_DB", "postgres")
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASSWORD = os.getenv("PG_PASSWORD", "")
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() == "true"

BATCH_SIZE = 1000

FETCH_SQL = """
    SELECT
        d.master_linkage_id,
        d.formulation_id,
        LOWER(TRIM(
            dml.combined_clean_jsonb
                -> 'dailymed'
                -> 'drug_info'
                -> 'products'
                -> 0
                ->> 'generic_name'
        )) AS new_name
    FROM drugdb.drug d
    JOIN public."DrugMasterLinkage" dml
        ON dml.master_linkage_id = d.master_linkage_id
    WHERE TRIM(COALESCE(d.generic_name, '')) = ''
      AND TRIM(COALESCE(
            dml.combined_clean_jsonb
                -> 'dailymed'
                -> 'drug_info'
                -> 'products'
                -> 0
                ->> 'generic_name',
            ''
          )) != ''
    LIMIT %s
"""

UPDATE_SQL = """
    UPDATE drugdb.drug
    SET generic_name = %s
    WHERE formulation_id = %s
      AND TRIM(COALESCE(generic_name, '')) = ''
"""

COUNT_EMPTY_SQL = """
    SELECT COUNT(*) FROM drugdb.drug WHERE TRIM(COALESCE(generic_name, '')) = ''
"""


def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"=== fix_generic_name.py [{mode}] ===")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute(COUNT_EMPTY_SQL)
            initial_empty = cur.fetchone()[0]
            print(f"Empty generic_name rows before: {initial_empty:,}")

        total_updated = 0

        while True:
            with conn.cursor() as cur:
                cur.execute(FETCH_SQL, (BATCH_SIZE,))
                batch = cur.fetchall()

            if not batch:
                break

            print(f"\nBatch of {len(batch)} rows to update:")
            for master_id, formulation_id, new_name in batch[:5]:
                print(f"  formulation_id={formulation_id} → '{new_name}'")
            if len(batch) > 5:
                print(f"  ... and {len(batch) - 5} more")

            if not DRY_RUN:
                with conn.cursor() as cur:
                    for _, formulation_id, new_name in batch:
                        cur.execute(UPDATE_SQL, (new_name, formulation_id))
                conn.commit()
                total_updated += len(batch)
                print(f"  Committed {len(batch)} updates (total so far: {total_updated:,})")
            else:
                print(f"  [DRY RUN] Would update {len(batch)} rows — not writing")
                break  # In dry run just show one batch preview then stop

        with conn.cursor() as cur:
            cur.execute(COUNT_EMPTY_SQL)
            final_empty = cur.fetchone()[0]

        print(f"\n=== Summary ===")
        if DRY_RUN:
            print(f"Rows that WOULD be updated: up to {initial_empty:,} (dry run — no changes made)")
        else:
            print(f"Rows updated:        {total_updated:,}")
            print(f"Empty before:        {initial_empty:,}")
            print(f"Empty after:         {final_empty:,}")
            print(f"Still empty:         {final_empty:,}")

    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
