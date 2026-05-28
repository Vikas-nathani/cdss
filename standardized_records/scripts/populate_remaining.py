#!/usr/bin/env python3
"""
Finish the remaining two sources: drugbank (clean_record) + rxnorm (record).
OpenFDA and DailyMed already completed successfully.
Uses OFFSET-based pagination instead of server-side cursors to avoid stall.
"""

import copy
import json
import logging
import os
import time
from datetime import timedelta
import psycopg2
import psycopg2.extras

DB = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432,
          user="postgres", password=os.environ.get("DB_PASSWORD", ""), database="postgres")
TABLE     = "DrugSourceMaster"
DST_COL   = "standardized_records"
PK        = "id"
BATCH     = 1000

logging.basicConfig(
    filename="transformation_errors.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s",
    filemode="a",
)

def progress_bar(done, total, t0, width=38):
    filled  = int(width * done / total) if total else 0
    bar     = "=" * filled + ">" + " " * max(0, width - filled - 1)
    pct     = 100 * done / total if total else 0
    elapsed = time.time() - t0
    speed   = done / elapsed if elapsed > 0 else 0
    rem     = (total - done) / speed if speed > 0 else 0
    eta     = str(timedelta(seconds=int(rem)))
    print(f"\r  [{bar}] {done:>8,}/{total:,} ({pct:5.1f}%) | "
          f"{speed:6.0f} rec/s | ETA {eta}   ", end="", flush=True)


def process_offset(conn, source, src_col, transform_fn, total):
    """
    Offset-based pagination: avoids server-side cursor stall.
    Fetches one batch at a time with LIMIT/OFFSET on id ordering.
    """
    processed = errors = 0
    t0 = time.time()

    # Get ordered list of PKs first (fast, small)
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {PK} FROM "{TABLE}" '
            f'WHERE source = %s AND {src_col} IS NOT NULL ORDER BY {PK}',
            (source,)
        )
        all_ids = [str(r[0]) for r in cur.fetchall()]

    total = len(all_ids)

    for i in range(0, total, BATCH):
        batch_ids = all_ids[i : i + BATCH]
        update_batch = []

        with conn.cursor() as cur:
            cur.execute(
                f'SELECT {PK}, {src_col} FROM "{TABLE}" '
                f'WHERE {PK} = ANY(%s::uuid[])',
                (batch_ids,)
            )
            rows = cur.fetchall()

        for pk, rec in rows:
            try:
                if isinstance(rec, str):
                    rec = json.loads(rec)
                out = transform_fn(rec)
                update_batch.append((json.dumps(out, default=str), str(pk)))
                processed += 1
            except Exception as exc:
                errors += 1
                logging.error("[%s] id=%s: %s", source, pk, exc)

        if update_batch:
            with conn.cursor() as upd:
                psycopg2.extras.execute_batch(
                    upd,
                    f'UPDATE "{TABLE}" SET {DST_COL} = %s::jsonb '
                    f'WHERE {PK} = %s::uuid',
                    update_batch,
                    page_size=BATCH,
                )
            conn.commit()

        progress_bar(processed, total, t0)

    elapsed = time.time() - t0
    print()
    return processed, errors, round(elapsed, 1)


def main():
    conn = psycopg2.connect(**DB)
    conn.autocommit = False

    print("Connecting… ✓\n")

    # Check what still needs population
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT source,
                   COUNT(*) AS total,
                   COUNT({DST_COL}) AS populated
            FROM "{TABLE}"
            WHERE source IN ('drugbank','rxnorm')
            GROUP BY source ORDER BY source
        """)
        counts = {r[0]: {"total": r[1], "populated": r[2]} for r in cur.fetchall()}

    for src, c in counts.items():
        missing = c["total"] - c["populated"]
        print(f"  {src:<12}: {c['populated']:>8,}/{c['total']:>8,} populated "
              f"({missing:,} remaining)")

    print()

    stats = {}
    plan = [
        ("drugbank", "clean_record", lambda r: copy.deepcopy(r)),
        ("rxnorm",   "record",       lambda r: copy.deepcopy(r)),
    ]

    for source, src_col, fn in plan:
        c = counts.get(source, {})
        total   = c.get("total", 0)
        already = c.get("populated", 0)

        if already == total:
            print(f"  {source.upper()}: already complete — skipping.")
            continue

        print(f"Copying {source.upper()} ({total:,} records from {src_col})…")
        done, errs, elapsed = process_offset(conn, source, src_col, fn, total)
        stats[source] = {"total": total, "processed": done,
                         "errors": errs, "seconds": elapsed}
        print(f"  ✓ {done:,}/{total:,} | {errs} errors | {elapsed}s")

    conn.close()

    # Verification
    print("\n── Verification ──────────────────────────────────")
    vconn = psycopg2.connect(**DB)
    with vconn.cursor() as cur:
        cur.execute(f"""
            SELECT source,
                   COUNT(*) AS total,
                   COUNT({DST_COL}) AS populated
            FROM "{TABLE}"
            GROUP BY source ORDER BY source
        """)
        for src, tot, pop in cur.fetchall():
            ok  = "✓" if tot == pop else "✗"
            pct = 100 * pop / tot if tot else 0
            print(f"  {ok} {(src or 'NULL'):<12} {pop:>10,}/{tot:,}  ({pct:.1f}%)")
    vconn.close()

    print("\n✓ Done.")

if __name__ == "__main__":
    main()
