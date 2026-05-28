#!/usr/bin/env python3
"""
Phase 5 Stage 1: Execute all 50 direct-mapping UPDATEs inside a single transaction.
Rolls back immediately on any error.
"""

import json
import os
import sys
from datetime import datetime

import psycopg2

DB = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     5432,
    "dbname":   "postgres",
    "user":     "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
}

PATTERNS_FILE = "dosage_form_regex_patterns.json"
LOG_FILE      = "stage1_execution_log.txt"


def run():
    with open(PATTERNS_FILE) as f:
        patterns = json.load(f)

    log_lines = []
    log_lines.append("=" * 80)
    log_lines.append("STAGE 1 UPDATE EXECUTION LOG")
    log_lines.append(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_lines.append("=" * 80)

    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute("BEGIN")
        log_lines.append("\n[BEGIN transaction]")

        total_updated = 0
        results = []

        for entry in patterns:
            df      = entry["dosage_form"]
            suffix  = entry["suffix"]
            pattern = entry["regex_pattern"]

            sql = """
                UPDATE drugdb.drug
                SET generic_formulation = trim(
                    regexp_replace(generic_formulation, %s, '', 'i')
                )
                WHERE dosage_forms = %s
                  AND generic_formulation ~* %s
            """
            cur.execute(sql, (pattern, df, pattern))
            rowcount = cur.rowcount
            total_updated += rowcount

            msg = f"  [{rowcount:>5} rows]  [{df}]  strip '{suffix}'"
            log_lines.append(msg)
            print(msg)
            results.append({"dosage_form": df, "suffix": suffix, "rows_updated": rowcount})

        log_lines.append(f"\n[TOTAL rows updated: {total_updated}]")
        print(f"\n  TOTAL rows updated: {total_updated}")

        # --- Sample 10 before/after from the backup comparison ---
        cur.execute("""
            SELECT
                formulation_id,
                dosage_forms,
                generic_formulation_original AS before_val,
                generic_formulation           AS after_val
            FROM drugdb.drug
            WHERE generic_formulation != generic_formulation_original
            LIMIT 10
        """)
        rows = cur.fetchall()
        log_lines.append("\n--- 10 sample before/after rows ---")
        for r in rows:
            log_lines.append(f"  ID {r[0]}  [{r[1]}]")
            log_lines.append(f"    BEFORE: {r[2]}")
            log_lines.append(f"    AFTER : {r[3]}")

        # --- Breakdown by dosage_form ---
        cur.execute("""
            SELECT dosage_forms, COUNT(*) AS changed
            FROM drugdb.drug
            WHERE generic_formulation != generic_formulation_original
            GROUP BY dosage_forms
            ORDER BY changed DESC
        """)
        breakdown = cur.fetchall()
        log_lines.append("\n--- Actual rows changed, by dosage_form ---")
        for b in breakdown:
            log_lines.append(f"  {b[1]:>6}  {b[0]}")

        conn.commit()
        log_lines.append(f"\n[COMMIT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        log_lines.append("Stage 1 completed successfully.")

        # Print report
        print("\n" + "=" * 60)
        print("10 SAMPLE BEFORE/AFTER (post-commit verification)")
        print("=" * 60)
        for r in rows:
            print(f"  ID {r[0]}  [{r[1]}]")
            print(f"    BEFORE: {r[2]}")
            print(f"    AFTER : {r[3]}")

        print("\n--- Breakdown by dosage_form ---")
        for b in breakdown:
            print(f"  {b[1]:>6}  {b[0]}")

        print(f"\nTOTAL rows modified: {total_updated}")

    except Exception as e:
        conn.rollback()
        msg = f"\n[ROLLBACK — ERROR: {e}]"
        log_lines.append(msg)
        print(msg, file=sys.stderr)
        sys.exit(1)
    finally:
        cur.close()
        conn.close()
        with open(LOG_FILE, "w") as f:
            f.write("\n".join(log_lines))
        print(f"\n[Log saved to {LOG_FILE}]")


if __name__ == "__main__":
    run()
