#!/usr/bin/env python3
"""
DrugSourceMaster – standardized_records column population
Phase 1: Test + preview (10 records)
Phase 2: Full population (all records)

Column name in DB: record  (not 'records')
Primary key:       id  (uuid)
"""

import copy
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras

# ── Config ───────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": 5432,
    "user": "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
    "database": "postgres",
}

TABLE       = "DrugSourceMaster"
SRC_COL     = "record"            # actual column name in DB
DST_COL     = "standardized_records"
PK_COL      = "id"
BATCH_SIZE  = 1000
DATA_DIR    = Path("/home/nathanivikas890_gmail_com/cdss/data")

logging.basicConfig(
    filename=str(Path(__file__).resolve().parent.parent / "logs" / "standardization_errors.log"),
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s [%(funcName)s] %(message)s",
)

# ── Template loading ──────────────────────────────────────────────────────────

def load_templates():
    with open(DATA_DIR / "normalized_schema.json") as f:
        openfda_tpl = json.load(f)
    with open(DATA_DIR / "master_schema_dailymed_normalized.json") as f:
        dailymed_tpl = json.load(f)
    return openfda_tpl, dailymed_tpl

# ── Deep merge ────────────────────────────────────────────────────────────────

def deep_merge(template, record):
    """
    Merge template (provides structure/defaults) with record (provides values).
    - Keys in both:  use record value, recursively merged
    - Key only in template:  use template value (preserves null structure)
    - Key only in record:  keep record value (zero data loss)
    Non-dict leaves: prefer record value if not None, else template value.
    """
    if isinstance(template, dict) and isinstance(record, dict):
        result = {}
        all_keys = set(template) | set(record)
        for key in all_keys:
            if key in template and key in record:
                result[key] = deep_merge(template[key], record[key])
            elif key in record:
                result[key] = copy.deepcopy(record[key])   # extra key from record
            else:
                result[key] = copy.deepcopy(template[key]) # null from template
        return result
    elif record is not None:
        return copy.deepcopy(record)
    else:
        return copy.deepcopy(template)

# ── Per-source transform ──────────────────────────────────────────────────────

def transform(source, record_data, openfda_tpl, dailymed_tpl):
    if source == "openfda":
        return deep_merge(openfda_tpl, record_data)
    elif source == "dailymed":
        return deep_merge(dailymed_tpl, record_data)
    else:
        # rxnorm / drugbank – exact copy, no structural change
        return copy.deepcopy(record_data)

# ── Progress bar ──────────────────────────────────────────────────────────────

def progress_bar(current, total, start_time, width=40):
    filled  = int(width * current / total) if total > 0 else 0
    bar     = "=" * filled + ">" + " " * max(0, width - filled - 1)
    pct     = 100 * current / total if total > 0 else 0
    elapsed = time.time() - start_time
    speed   = current / elapsed if elapsed > 0 else 0
    rem     = (total - current) / speed if speed > 0 else 0
    eta     = str(timedelta(seconds=int(rem)))
    print(
        f"\r  [{bar}] {current:>8,}/{total:,} ({pct:5.1f}%) | "
        f"{speed:6.0f} rec/sec | ETA: {eta}   ",
        end="", flush=True,
    )

# ── Phase 1 ──────────────────────────────────────────────────────────────────

def phase1(conn, openfda_tpl, dailymed_tpl):
    print("\n" + "=" * 60)
    print("PHASE 1: SETUP AND TESTING (10 records)")
    print("=" * 60)

    with conn.cursor() as cur:

        # 1. Add column if absent
        cur.execute(f"""
            ALTER TABLE "{TABLE}"
            ADD COLUMN IF NOT EXISTS {DST_COL} JSONB;
        """)
        conn.commit()
        print(f"\n✓ Column '{DST_COL}' ensured on {TABLE}")

        # 2. Source distribution
        cur.execute(f"""
            SELECT source, COUNT(*)
            FROM "{TABLE}"
            WHERE {SRC_COL} IS NOT NULL
            GROUP BY source
            ORDER BY COUNT(*) DESC;
        """)
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) FROM \"{TABLE}\" WHERE {SRC_COL} IS NULL;")
        null_count = cur.fetchone()[0]

        print("\nSource Distribution:")
        source_counts = {}
        for src, cnt in rows:
            print(f"  - {src:<12}: {cnt:>10,} records")
            source_counts[src] = cnt
        print(f"  - {'NULL':<12}: {null_count:>10,} records")

        # 3. Fetch 2 sample records per source and preview transformation
        print("\n" + "-" * 60)
        print("Testing with 2 records per source...")
        print("-" * 60)

        sources = ["openfda", "dailymed", "rxnorm", "drugbank"]
        sample_output = {}

        for src in sources:
            cur.execute(f"""
                SELECT {PK_COL}, {SRC_COL}
                FROM "{TABLE}"
                WHERE source = %s AND {SRC_COL} IS NOT NULL
                LIMIT 2;
            """, (src,))
            sample_rows = cur.fetchall()

            if not sample_rows:
                print(f"\n[{src.upper()}] → No records found")
                continue

            print(f"\n[{src.upper()}]  ({len(sample_rows)} sample records)")
            sample_output[src] = []

            for pk, rec in sample_rows:
                if isinstance(rec, str):
                    rec = json.loads(rec)

                before_keys = sorted(rec.keys()) if isinstance(rec, dict) else ["<non-dict>"]
                merged      = transform(src, rec, openfda_tpl, dailymed_tpl)
                after_keys  = sorted(merged.keys()) if isinstance(merged, dict) else ["<non-dict>"]

                # Keys added by template (structure keys now present)
                added_by_tpl = [k for k in after_keys if k not in before_keys]
                # Keys present in both (data carried over)
                carried_over = [k for k in after_keys if k in before_keys]
                # Keys only in raw record (extra, not in template)
                extra_raw    = [k for k in before_keys if k not in after_keys] if src in ("openfda","dailymed") else []

                print(f"  Record {pk}")
                print(f"    Before  ({len(before_keys)} top-level keys): {before_keys}")
                print(f"    After   ({len(after_keys)} top-level keys): {after_keys}")
                if added_by_tpl:
                    print(f"    ↳ Added by template (null-padded): {added_by_tpl}")
                if carried_over:
                    print(f"    ↳ Carried over from record:        {carried_over}")

                sample_output[src].append({
                    "id": str(pk),
                    "before": rec,
                    "after": merged,
                    "stats": {
                        "before_top_keys": len(before_keys),
                        "after_top_keys":  len(after_keys),
                        "added_by_template": added_by_tpl,
                        "carried_from_record": carried_over,
                    },
                })

        # 4. Save sample file
        with open("sample_transformations.json", "w") as f:
            json.dump(sample_output, f, indent=2, default=str)
        print(f"\n✓ Sample transformations saved → sample_transformations.json")

    return source_counts, null_count

# ── Phase 2 ──────────────────────────────────────────────────────────────────

def phase2(conn, openfda_tpl, dailymed_tpl, source_counts, null_count):
    print("\n" + "=" * 60)
    print("PHASE 2: FULL PROCESSING")
    print("=" * 60)

    all_stats  = {}
    total_wall = time.time()
    total_err  = 0

    sources_cfg = [
        ("openfda",  "transform"),
        ("dailymed", "transform"),
        ("rxnorm",   "copy"),
        ("drugbank", "copy"),
    ]

    for src, mode in sources_cfg:
        total = source_counts.get(src, 0)
        if total == 0:
            print(f"\n[{src.upper()}] No records – skipping.")
            continue

        label = "Copying" if mode == "copy" else "Processing"
        print(f"\n{label} {src.upper()} ({total:,} records)...")

        src_stats = {
            "total_records": total,
            "processed": 0,
            "errors": 0,
            "processing_time_seconds": 0,
        }
        processed = errors = 0
        t_start = time.time()

        # Server-side cursor for memory-efficient streaming
        with conn.cursor(name=f"stream_{src}") as cur:
            cur.execute(f"""
                SELECT {PK_COL}, {SRC_COL}
                FROM "{TABLE}"
                WHERE source = %s AND {SRC_COL} IS NOT NULL
                ORDER BY {PK_COL};
            """, (src,))

            batch = []
            while True:
                chunk = cur.fetchmany(BATCH_SIZE)
                if not chunk:
                    break

                for pk, rec in chunk:
                    try:
                        if isinstance(rec, str):
                            rec = json.loads(rec)
                        merged = transform(src, rec, openfda_tpl, dailymed_tpl)
                        batch.append((json.dumps(merged, default=str), pk))
                        processed += 1
                    except Exception as exc:
                        errors += 1
                        total_err += 1
                        logging.error(f"[{src}] id={pk}: {exc}")

                if batch:
                    with conn.cursor() as upd:
                        psycopg2.extras.execute_batch(
                            upd,
                            f'UPDATE "{TABLE}" SET {DST_COL} = %s::jsonb WHERE {PK_COL} = %s::uuid',
                            batch,
                            page_size=BATCH_SIZE,
                        )
                    conn.commit()
                    batch = []

                progress_bar(processed, total, t_start)

        elapsed = time.time() - t_start
        src_stats["processed"] = processed
        src_stats["errors"]    = errors
        src_stats["processing_time_seconds"] = round(elapsed, 1)
        if mode == "copy":
            src_stats["copied"] = processed
        all_stats[src] = src_stats

        print()   # newline after progress bar
        print(f"  ✓ {processed:,}/{total:,} {('copied' if mode=='copy' else 'processed')} "
              f"| {errors} errors | {elapsed:.1f}s")

    all_stats["null_records"] = null_count
    all_stats["total_processing_time_seconds"] = round(time.time() - total_wall, 1)

    with open("standardization_stats.json", "w") as f:
        json.dump(all_stats, f, indent=2)

    # Final summary
    total_elapsed = all_stats["total_processing_time_seconds"]
    print("\n" + "=" * 60)
    print("COMPLETION SUMMARY")
    print("=" * 60)
    for src, s in all_stats.items():
        if not isinstance(s, dict):
            continue
        proc  = s.get("processed", s.get("copied", 0))
        total = s.get("total_records", 0)
        errs  = s.get("errors", 0)
        verb  = "copied" if src in ("rxnorm", "drugbank") else "processed"
        print(f"  ✓ {src.upper():<12}: {proc:>10,}/{total:,} {verb} ({errs} errors)")
    print(f"  ✓ {'NULL':<12}: {null_count:>10,} skipped")
    print(f"  Total time: {timedelta(seconds=int(total_elapsed))}")
    print(f"  Total errors: {total_err}  (see standardization_errors.log)")

    # DB verification query
    print("\nDatabase Verification:")
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT source,
                   COUNT(*)                   AS total,
                   COUNT({DST_COL})           AS populated
            FROM "{TABLE}"
            GROUP BY source
            ORDER BY source;
        """)
        for src, tot, pop in cur.fetchall():
            check = "✓" if tot == pop else "✗"
            print(f"  {check} {(src or 'NULL'):<12}: {pop:>10,}/{tot:,} populated")

    print(f"\n✓ standardization_stats.json saved")
    print(f"✓ standardization_errors.log written")

# ── Entry point ───────────────────────────────────────────────────────────────

def main(run_phase2=False):
    print("Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        print(f"✓ Connected to {DB_CONFIG['host']} / {DB_CONFIG['database']}")
    except Exception as exc:
        print(f"✗ Connection failed: {exc}")
        sys.exit(1)

    print("Loading schema templates...")
    openfda_tpl, dailymed_tpl = load_templates()
    print(f"  ✓ OpenFDA template:  {len(openfda_tpl)} top-level categories")
    print(f"  ✓ DailyMed template: {len(dailymed_tpl)} top-level categories")

    try:
        source_counts, null_count = phase1(conn, openfda_tpl, dailymed_tpl)

        if run_phase2:
            phase2(conn, openfda_tpl, dailymed_tpl, source_counts, null_count)
        else:
            print("\n" + "=" * 60)
            print("Phase 1 complete. Review results above and in:")
            print("  → sample_transformations.json")
            print("Confirm Phase 2 to populate all records.")
            print("=" * 60)
    finally:
        conn.close()


if __name__ == "__main__":
    # Pass --phase2 flag to trigger full population
    run_p2 = "--phase2" in sys.argv
    main(run_phase2=run_p2)
