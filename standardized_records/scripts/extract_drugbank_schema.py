#!/usr/bin/env python3
"""
DrugBank Schema Extractor
Phase 1: Test with 2 records
Phase 2: Full extraction (run with --full flag)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

import psycopg2
import psycopg2.extras

# ── DB config ────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
}

TABLE   = "DrugSourceMaster"
COLUMN  = "clean_record"
SOURCE  = "drugbank"

# ── Output paths ─────────────────────────────────────────────────────────────
OUT_DIR             = "/home/nathanivikas890_gmail_com/cdss/data"
SCHEMA_FILE         = f"{OUT_DIR}/master_schema_drugbank.json"
STATS_FILE          = f"{OUT_DIR}/schema_stats_drugbank.json"
CATALOG_FILE        = f"{OUT_DIR}/drugbank_field_catalog.txt"
ERROR_LOG           = f"{OUT_DIR}/drugbank_extraction_errors.log"


# ── Schema merging helpers ───────────────────────────────────────────────────

def merge_value(existing, incoming):
    """Merge an incoming value into an existing schema node."""
    # Both dicts → recurse
    if isinstance(existing, dict) and isinstance(incoming, dict):
        for k, v in incoming.items():
            if k in existing:
                existing[k] = merge_value(existing[k], v)
            else:
                existing[k] = extract_schema(v)
        return existing

    # Existing is dict but incoming is not → keep dict
    if isinstance(existing, dict):
        return existing

    # Incoming is dict but existing is not → promote to dict
    if isinstance(incoming, dict):
        return extract_schema(incoming)

    # Both lists → merge element schemas
    if isinstance(existing, list) and isinstance(incoming, list):
        if not existing:
            if incoming:
                return [extract_schema(incoming[0])]
            return []
        if incoming:
            existing[0] = merge_value(existing[0], incoming[0])
        return existing

    # Existing list, incoming not list → keep list
    if isinstance(existing, list):
        return existing

    # Incoming list, existing not list → promote
    if isinstance(incoming, list):
        if incoming:
            return [extract_schema(incoming[0])]
        return []

    # Scalar → keep None as placeholder
    return None


def extract_schema(value):
    """Build an initial schema template from a value."""
    if isinstance(value, dict):
        return {k: extract_schema(v) for k, v in value.items()}
    if isinstance(value, list):
        if value:
            return [extract_schema(value[0])]
        return []
    return None


def merge_record(schema: dict, record: dict):
    """Merge one record's structure into the running schema dict."""
    for k, v in record.items():
        if k in schema:
            schema[k] = merge_value(schema[k], v)
        else:
            schema[k] = extract_schema(v)


# ── Field-path enumeration ───────────────────────────────────────────────────

def collect_paths(node, prefix="", paths=None, depth=0):
    """Recursively collect all dot-notation paths from a schema node."""
    if paths is None:
        paths = set()
    if isinstance(node, dict):
        for k, v in node.items():
            full = f"{prefix}.{k}" if prefix else k
            paths.add(full)
            collect_paths(v, full, paths, depth + 1)
    elif isinstance(node, list) and node:
        collect_paths(node[0], prefix, paths, depth + 1)
    return paths


def max_depth(node, depth=0):
    if isinstance(node, dict) and node:
        return max(max_depth(v, depth + 1) for v in node.values())
    if isinstance(node, list) and node:
        return max_depth(node[0], depth + 1)
    return depth


def count_types(node, counts=None):
    if counts is None:
        counts = {"string": 0, "array": 0, "object": 0, "number": 0, "boolean": 0, "null": 0}
    if isinstance(node, dict):
        counts["object"] += 1
        for v in node.values():
            count_types(v, counts)
    elif isinstance(node, list):
        counts["array"] += 1
        if node:
            count_types(node[0], counts)
    else:
        counts["null"] += 1   # placeholders are null
    return counts


# ── Pretty progress bar ──────────────────────────────────────────────────────

def progress_bar(done, total, rate, eta_s, width=40):
    pct  = done / total if total else 0
    fill = int(width * pct)
    bar  = "=" * fill + ">" + " " * (width - fill - 1)
    eta  = f"{int(eta_s//60)}m {int(eta_s%60)}s" if eta_s < 3600 else f"{eta_s/3600:.1f}h"
    print(
        f"\r[{bar}] {done:,}/{total:,} ({pct*100:.1f}%) "
        f"| {rate:.0f} rec/sec | ETA: {eta}",
        end="", flush=True
    )


# ── Database helpers ─────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_count(cur):
    cur.execute(
        f"SELECT COUNT(*) FROM \"{TABLE}\" "
        f"WHERE source = %s AND {COLUMN} IS NOT NULL",
        (SOURCE,)
    )
    return cur.fetchone()[0]


def parse_record(raw):
    """Parse a record that may be str or already dict."""
    if isinstance(raw, str):
        return json.loads(raw)
    return raw  # psycopg2 already parsed JSON


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1
# ═══════════════════════════════════════════════════════════════════════════════

def phase1():
    print("\n====================================")
    print("PHASE 1: TESTING (2 records)")
    print("====================================\n")

    conn = get_connection()
    cur  = conn.cursor()

    # Count
    total = get_count(cur)
    print(f"Found {total:,} DrugBank records in {COLUMN} column\n")

    # Fetch 2
    print("Fetching 2 sample records...\n")
    cur.execute(
        f"SELECT {COLUMN} FROM \"{TABLE}\" "
        f"WHERE source = %s AND {COLUMN} IS NOT NULL LIMIT 2",
        (SOURCE,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        print("ERROR: No records returned.")
        sys.exit(1)

    records = []
    for i, (raw,) in enumerate(rows, 1):
        rec = parse_record(raw)
        records.append(rec)
        print(f"Record {i}:")
        print(json.dumps(rec, indent=2, default=str)[:4000])  # first ~4000 chars
        print()

    # Build schema from 2 records
    schema = {}
    for rec in records:
        merge_record(schema, rec)

    paths      = collect_paths(schema)
    top_fields = list(schema.keys())
    depth      = max_depth(schema)

    print(f"Schema extracted from 2 records:")
    print(f"  - Total unique fields : {len(paths)}")
    print(f"  - Top-level fields    : {len(top_fields)}")
    print(f"  - Max nesting depth   : {depth}\n")

    sample_paths = sorted(paths)[:20]
    print("Sample field paths (first 20):")
    for j, p in enumerate(sample_paths, 1):
        print(f"  {j:2}. {p}")

    print(f"\n  ... (showing first {len(sample_paths)} of {len(paths)} total paths)")
    print()

    # Summary object (printed only)
    summary = {
        "total_unique_fields": len(paths),
        "top_level_fields": len(top_fields),
        "max_nesting_depth": depth,
        "sample_field_paths": sample_paths,
    }
    print("Schema summary JSON:")
    print(json.dumps(summary, indent=2))
    print()
    print("Schema looks correct from 2 records. Proceed to full extraction? (Y/N)")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2
# ═══════════════════════════════════════════════════════════════════════════════

def phase2():
    print("\n====================================")
    print("PHASE 2: FULL EXTRACTION")
    print("====================================\n")

    import os
    os.makedirs(OUT_DIR, exist_ok=True)

    conn   = get_connection()
    cur    = conn.cursor("drugbank_cursor", cursor_factory=psycopg2.extras.DictCursor)

    count_cur = conn.cursor()
    total     = get_count(count_cur)
    count_cur.close()
    print(f"Processing all {total:,} DrugBank records...\n")

    cur.execute(
        f"SELECT {COLUMN} FROM \"{TABLE}\" "
        f"WHERE source = %s AND {COLUMN} IS NOT NULL",
        (SOURCE,)
    )

    schema       = {}
    processed    = 0
    parse_errors = 0
    start_time   = time.time()
    error_lines  = []

    BATCH = 1000

    for (raw,) in cur:
        try:
            rec = parse_record(raw)
            merge_record(schema, rec)
        except Exception as e:
            parse_errors += 1
            error_lines.append(f"[{datetime.now().isoformat()}] Record {processed+1}: {e}\n")
            continue
        finally:
            processed += 1

        if processed % BATCH == 0 or processed == total:
            elapsed = time.time() - start_time
            rate    = processed / elapsed if elapsed > 0 else 0
            eta     = (total - processed) / rate if rate > 0 else 0
            progress_bar(processed, total, rate, eta)

    print()  # newline after progress bar

    cur.close()
    conn.close()

    elapsed_total = time.time() - start_time

    # ── Write error log ──────────────────────────────────────────────────────
    if error_lines:
        with open(ERROR_LOG, "w") as f:
            f.writelines(error_lines)

    # ── Compute stats ────────────────────────────────────────────────────────
    paths      = collect_paths(schema)
    top_fields = list(schema.keys())
    depth      = max_depth(schema)
    nested     = len(paths) - len(top_fields)
    type_dist  = count_types(schema)
    # Remove the inflated "object"/"array" self-counts; use path counts for objects/arrays
    # Keep raw type distribution from placeholder traversal for field type hints
    common_top = [k for k in top_fields if not isinstance(schema.get(k), (dict, list))]
    common_top += [k for k in top_fields if isinstance(schema.get(k), dict)]
    common_top  = top_fields[:20]  # just first 20 top-level

    stats = {
        "total_records_processed": processed,
        "total_unique_fields": len(paths),
        "top_level_fields": len(top_fields),
        "nested_fields": nested,
        "max_nesting_depth": depth,
        "parse_errors": parse_errors,
        "extraction_time_seconds": round(elapsed_total, 2),
        "field_type_distribution": {
            "object": type_dist["object"],
            "array":  type_dist["array"],
            "null_placeholder": type_dist["null"],
        },
        "common_top_level_fields": common_top,
    }

    # ── Write master schema ──────────────────────────────────────────────────
    with open(SCHEMA_FILE, "w") as f:
        json.dump(schema, f, indent=2, default=str)

    # ── Write stats ──────────────────────────────────────────────────────────
    with open(STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2)

    # ── Write human-readable catalog ─────────────────────────────────────────
    _write_catalog(schema, paths, top_fields, depth, processed)

    # ── Console summary ──────────────────────────────────────────────────────
    print(f"\nExtraction complete!")
    print(f"  - Records processed : {processed:,}")
    print(f"  - Parse errors      : {parse_errors}")
    print(f"  - Total unique fields: {len(paths)}")
    print(f"  - Max nesting depth : {depth}")
    m, s = divmod(int(elapsed_total), 60)
    print(f"  - Processing time   : {m}m {s}s\n")

    print("Files saved:")
    print(f"  ✓ {SCHEMA_FILE}")
    print(f"  ✓ {STATS_FILE}")
    print(f"  ✓ {CATALOG_FILE}")
    if error_lines:
        print(f"  ✗ {parse_errors} errors logged to {ERROR_LOG}")


def _write_catalog(schema, all_paths, top_fields, depth, total):
    """Write the human-readable field catalog."""
    lines = []
    lines.append("====================================")
    lines.append("DRUGBANK SCHEMA FIELD CATALOG")
    lines.append("====================================")
    lines.append(f"Total Fields : {len(all_paths)}")
    lines.append(f"Max Depth    : {depth} levels")
    lines.append(f"Records used : {total:,}")
    lines.append("")

    lines.append(f"TOP-LEVEL FIELDS ({len(top_fields)}):")
    for k in sorted(top_fields):
        lines.append(f"  - {k}")
    lines.append("")

    # Group remaining paths by their top-level key
    sections = {}
    for p in sorted(all_paths):
        parts = p.split(".")
        if len(parts) > 1:
            top = parts[0]
            sections.setdefault(top, []).append(p)

    for section, spaths in sorted(sections.items()):
        lines.append(f"{section.upper()} SECTION ({len(spaths)} fields):")
        for p in spaths:
            lines.append(f"  - {p}")
        lines.append("")

    with open(CATALOG_FILE, "w") as f:
        f.write("\n".join(lines))


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Run Phase 2 full extraction")
    args = parser.parse_args()

    if args.full:
        phase2()
    else:
        phase1()

