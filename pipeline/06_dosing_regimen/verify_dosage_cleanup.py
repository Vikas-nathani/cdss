#!/usr/bin/env python3
"""
Phase 4: Dry-run verification of dosage form suffix cleanup.
Reads dosage_form_regex_patterns.json and generates a full report
WITHOUT modifying any data.
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
REPORT_FILE   = "verification_report.txt"


def connect():
    return psycopg2.connect(**DB)


def total_rows(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM drugdb.drug")
    return cur.fetchone()[0]


def count_affected(cur, dosage_form: str, pattern: str) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM drugdb.drug "
        "WHERE dosage_forms = %s AND generic_formulation ~* %s",
        (dosage_form, pattern),
    )
    return cur.fetchone()[0]


def sample_preview(cur, dosage_form: str, pattern: str, limit: int = 5) -> list[dict]:
    cur.execute(
        """
        SELECT
            formulation_id,
            dosage_forms,
            generic_formulation AS before_value,
            trim(regexp_replace(generic_formulation, %s, '', 'i')) AS after_value
        FROM drugdb.drug
        WHERE dosage_forms = %s
          AND generic_formulation ~* %s
        LIMIT %s
        """,
        (pattern, dosage_form, pattern, limit),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def main():
    with open(PATTERNS_FILE) as f:
        patterns = json.load(f)

    conn = connect()
    cur  = conn.cursor()

    total = total_rows(cur)

    lines = []
    lines.append("=" * 80)
    lines.append("DOSAGE FORM CLEANUP — VERIFICATION REPORT (DRY RUN)")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 80)
    lines.append(f"\nTotal rows in drugdb.drug: {total:,}")
    lines.append("")

    grand_total_affected = 0
    per_pattern_results  = []

    for entry in patterns:
        df      = entry["dosage_form"]
        suffix  = entry["suffix"]
        pattern = entry["regex_pattern"]

        count   = count_affected(cur, df, pattern)
        samples = sample_preview(cur, df, pattern)

        grand_total_affected += count
        per_pattern_results.append({
            "dosage_form": df,
            "suffix":      suffix,
            "pattern":     pattern,
            "count":       count,
            "samples":     samples,
        })

    lines.append(f"Total rows that WILL be updated: {grand_total_affected:,}")
    lines.append(f"Rows that will remain unchanged:  {total - grand_total_affected:,}")
    lines.append("")
    lines.append("-" * 80)
    lines.append("BREAKDOWN BY DOSAGE FORM")
    lines.append("-" * 80)

    for r in sorted(per_pattern_results, key=lambda x: -x["count"]):
        if r["count"] == 0:
            continue
        lines.append(f"\n  dosage_forms : {r['dosage_form']}")
        lines.append(f"  strip suffix : '{r['suffix']}'")
        lines.append(f"  regex        : {r['pattern']}")
        lines.append(f"  affected rows: {r['count']:,}")
        lines.append("  sample before → after:")
        for s in r["samples"]:
            lines.append(f"    ID {s['formulation_id']}")
            lines.append(f"      BEFORE: {s['before_value']}")
            lines.append(f"      AFTER : {s['after_value']}")

    lines.append("")
    lines.append("-" * 80)
    lines.append("ZERO-MATCH PATTERNS (no rows affected, skipped in UPDATE)")
    lines.append("-" * 80)
    zero = [r for r in per_pattern_results if r["count"] == 0]
    if zero:
        for r in zero:
            lines.append(f"  {r['dosage_form']}  (suffix: '{r['suffix']}')")
    else:
        lines.append("  None — all patterns matched at least one row.")

    lines.append("")
    lines.append("=" * 80)
    lines.append("END OF REPORT — awaiting your approval before any UPDATE is run.")
    lines.append("=" * 80)

    report = "\n".join(lines)

    with open(REPORT_FILE, "w") as f:
        f.write(report)

    print(report)
    print(f"\n[saved to {REPORT_FILE}]")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
