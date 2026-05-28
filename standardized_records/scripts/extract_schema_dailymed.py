#!/usr/bin/env python3
"""
DailyMed Master Schema Extractor
Extracts all unique field paths from DailyMed records in PostgreSQL,
preserving nested structure into a master schema template.
"""

import json
import os
import sys
import time
import psycopg2
import psycopg2.extras

# ─── Database Configuration ──────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "user":     "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
    "dbname":   "postgres",
    "port":     5432,
}

OUTPUT_SCHEMA = "master_schema_dailymed.json"
OUTPUT_STATS  = "schema_stats_dailymed.json"


# ─── Schema Utilities ─────────────────────────────────────────────────────────

def merge_into_schema(schema: dict, record: dict, depth: int = 1) -> int:
    """
    Recursively merge a record's structure into the running schema template.
    Returns the maximum nesting depth encountered in this record.
    """
    max_depth = depth
    for key, value in record.items():
        if isinstance(value, dict):
            if key not in schema or not isinstance(schema[key], dict):
                schema[key] = {}
            child_depth = merge_into_schema(schema[key], value, depth + 1)
            max_depth = max(max_depth, child_depth)
        elif isinstance(value, list):
            # Inspect list elements for nested objects
            if key not in schema or not isinstance(schema[key], dict):
                schema[key] = {}
            for item in value:
                if isinstance(item, dict):
                    child_depth = merge_into_schema(schema[key], item, depth + 1)
                    max_depth = max(max_depth, child_depth)
        else:
            if key not in schema:
                schema[key] = None
    return max_depth


def count_paths(schema: dict, current_depth: int = 1):
    """
    Returns (total_fields, top_level_fields, nested_fields, max_depth).
    """
    top_level = 0
    nested    = 0
    max_depth = current_depth

    for key, value in schema.items():
        if current_depth == 1:
            top_level += 1
        else:
            nested += 1

        if isinstance(value, dict):
            _, child_top, child_nested, child_max = count_paths(value, current_depth + 1)
            nested    += child_top + child_nested
            max_depth  = max(max_depth, child_max)

    total = top_level + nested
    return total, top_level, nested, max_depth


def print_schema_preview(schema: dict, indent: int = 0, max_lines: int = 60):
    """Pretty-print the first max_lines of the schema."""
    lines = json.dumps(schema, indent=2).splitlines()
    shown = lines[:max_lines]
    for line in shown:
        print(line)
    if len(lines) > max_lines:
        print(f"  ... ({len(lines) - max_lines} more lines) ...")


# ─── Progress Bar ─────────────────────────────────────────────────────────────

def progress_bar(current: int, total: int, bar_width: int = 40, elapsed: float = 0.0):
    filled  = int(bar_width * current / total)
    bar     = "█" * filled + "░" * (bar_width - filled)
    pct     = current / total * 100
    recs_s  = current / elapsed if elapsed > 0 else 0
    eta     = (total - current) / recs_s if recs_s > 0 else 0
    sys.stdout.write(
        f"\r  [{bar}] {pct:5.1f}%  {current:,}/{total:,}  "
        f"{recs_s:,.0f} rec/s  ETA {eta:,.0f}s   "
    )
    sys.stdout.flush()


# ─── Database Helpers ─────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_records(cursor, limit: int = None):
    sql = """
        SELECT clean_record
        FROM   "DrugSourceMaster"
        WHERE  source = 'dailymed'
          AND  clean_record IS NOT NULL
    """
    if limit:
        sql += f" LIMIT {limit}"
    cursor.execute(sql)
    return cursor.fetchall()


# ─── Phase 1 ──────────────────────────────────────────────────────────────────

def phase1():
    print("=" * 60)
    print("PHASE 1 — Test with 2 records")
    print("=" * 60)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            rows = fetch_records(cur, limit=2)

        print(f"  Fetched {len(rows)} record(s) from database.\n")

        schema    = {}
        max_depth = 1
        errors    = 0

        for i, (raw,) in enumerate(rows, 1):
            try:
                record = raw if isinstance(raw, dict) else json.loads(raw)
                depth  = merge_into_schema(schema, record)
                max_depth = max(max_depth, depth)
                print(f"  Record {i}: {len(record)} top-level keys  "
                      f"(keys: {', '.join(list(record.keys())[:8])}{'...' if len(record) > 8 else ''})")
            except (json.JSONDecodeError, TypeError) as e:
                errors += 1
                print(f"  Record {i}: PARSE ERROR — {e}")

        total, top, nested, _ = count_paths(schema)

        print(f"\n  ── Schema from 2 records ──")
        print(f"  Top-level fields : {top}")
        print(f"  Nested fields    : {nested}")
        print(f"  Max depth        : {max_depth}")
        print(f"  Parse errors     : {errors}")
        print()
        print("  ── Preview ──")
        print_schema_preview(schema, max_lines=80)

    finally:
        conn.close()

    print()
    print("Phase 1 complete.")
    print("Run the script with argument  --phase2  to process all 51,731 records.")


# ─── Phase 2 ──────────────────────────────────────────────────────────────────

def phase2():
    print("=" * 60)
    print("PHASE 2 — Full extraction (51,731 records)")
    print("=" * 60)

    conn = get_connection()
    schema    = {}
    max_depth = 1
    errors    = 0
    processed = 0
    start     = time.time()

    try:
        # Use server-side cursor to stream rows without loading all into RAM
        with conn.cursor("schema_cursor", cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.itersize = 500  # fetch 500 rows at a time from server
            sql = """
                SELECT clean_record
                FROM   "DrugSourceMaster"
                WHERE  source = 'dailymed'
                  AND  clean_record IS NOT NULL
            """
            cur.execute(sql)
            total_est = 51731

            print(f"  Streaming records (batch size 500)…\n")

            for (raw,) in cur:
                try:
                    record = raw if isinstance(raw, dict) else json.loads(raw)
                    depth  = merge_into_schema(schema, record)
                    max_depth = max(max_depth, depth)
                except (json.JSONDecodeError, TypeError):
                    errors += 1

                processed += 1
                if processed % 250 == 0 or processed == total_est:
                    progress_bar(processed, total_est, elapsed=time.time() - start)

    finally:
        conn.close()

    elapsed = time.time() - start
    progress_bar(processed, processed, elapsed=elapsed)
    print()  # newline after progress bar

    # ── Compute stats ─────────────────────────────────────────────────────────
    total_fields, top_fields, nested_fields, _ = count_paths(schema)

    stats = {
        "total_records_processed": processed,
        "total_unique_fields":     total_fields,
        "top_level_fields":        top_fields,
        "nested_fields":           nested_fields,
        "max_nesting_depth":       max_depth,
        "parse_errors":            errors,
        "extraction_time_seconds": round(elapsed, 2),
    }

    # ── Save outputs ──────────────────────────────────────────────────────────
    with open(OUTPUT_SCHEMA, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)

    with open(OUTPUT_STATS, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print()
    print("  ── Extraction Complete ──")
    for k, v in stats.items():
        print(f"  {k:<35}: {v}")
    print()
    print(f"  Schema saved to : {OUTPUT_SCHEMA}")
    print(f"  Stats saved to  : {OUTPUT_STATS}")


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--phase2" in sys.argv:
        phase2()
    else:
        phase1()
