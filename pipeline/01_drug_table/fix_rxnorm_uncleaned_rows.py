#!/usr/bin/env python3
"""
fix_rxnorm_uncleaned_rows.py

Patches the 17,128 drug rows that were left with NULL rxcui /
rxnorm_generic_formulation because their formulation_id was generated
from the original UNCLEANED formulation string (before the dosage form
suffix cleanup migration of 2026-05-02).

Root cause:
  populate_drug_table.py originally had a buggy strip_dosage_form_suffix()
  that only did exact-string matching.  For EU/uppercase dosage form codes
  (e.g. "TABLET, EXTENDED RELEASE") the suffix was NOT stripped, so the UUID
  seed contained the full uncleaned string.  The later dosage form cleanup
  corrected generic_formulation but the formulation_id UUID was not changed.

  update_drug_rxnorm_columns.py recomputes the UUID from the CLEANED
  formulation → misses these 17,128 rows.

Fix strategy:
  1. Load the 17,128 NULL rows from the drug table into memory, keyed by
     (master_linkage_id, generic_formulation_original, dosage_forms).
     generic_formulation_original stores the exact uncleaned value that was
     used to generate the UUID, so this key will match DrugMasterLinkage data.
  2. Stream DrugMasterLinkage and look up each rxnorm entry by
     (master_linkage_id, raw_formulation, dosage_form).
  3. UPDATE the matched rows with rxcui + rxnorm_generic_formulation.

Usage:
  python3 fix_rxnorm_uncleaned_rows.py --password $DB_PASSWORD
  python3 fix_rxnorm_uncleaned_rows.py --password $DB_PASSWORD --dry-run --verbose
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras


# ──────────────────────────────────────────────────────────────
# Load NULL rows into memory
# ──────────────────────────────────────────────────────────────

LOAD_NULL_ROWS_SQL = """
    SELECT
        formulation_id::text,
        master_linkage_id::text,
        generic_formulation_original,
        dosage_forms
    FROM drugdb.drug
    WHERE rxcui IS NULL
       OR rxnorm_generic_formulation IS NULL
"""

FETCH_SQL = """
    SELECT master_linkage_id::text, combined_clean_jsonb
    FROM   public."DrugMasterLinkage"
    WHERE  combined_clean_jsonb IS NOT NULL
"""

UPDATE_SQL = """
    UPDATE drugdb.drug
    SET    rxcui                      = %s,
           rxnorm_generic_formulation = %s
    WHERE  formulation_id = %s::uuid
"""


@dataclass
class Stats:
    null_rows_loaded:        int = 0
    source_records_read:     int = 0
    rxnorm_entries_checked:  int = 0
    successful_matches:      int = 0
    rows_updated:            int = 0
    errors:                  int = 0
    error_details: list = field(default_factory=list)


def build_lookup(conn, log: logging.Logger, stats: Stats) -> dict:
    """
    Returns dict: (master_linkage_id, generic_formulation_original, dosage_forms)
                  → formulation_id
    """
    lookup = {}
    with conn.cursor() as cur:
        cur.execute(LOAD_NULL_ROWS_SQL)
        for row in cur.fetchall():
            formulation_id, master_linkage_id, gf_original, dosage_forms = row
            if not gf_original:
                continue
            key = (master_linkage_id, gf_original.strip(), (dosage_forms or "").strip())
            lookup[key] = formulation_id
    stats.null_rows_loaded = len(lookup)
    log.info("Loaded %d NULL rows into lookup dict.", len(lookup))
    return lookup


def run_fix(read_conn, write_conn, lookup: dict, dry_run: bool,
            batch_size: int, verbose: bool, log: logging.Logger, stats: Stats):

    src_cur = read_conn.cursor(
        name="fix_uncleaned_stream",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    src_cur.itersize = 500
    src_cur.execute(FETCH_SQL)

    write_cur = write_conn.cursor()
    batch: list[tuple] = []

    def flush_batch():
        if not batch or dry_run:
            batch.clear()
            return
        try:
            write_cur.executemany(UPDATE_SQL, batch)
            write_conn.commit()
            log.info("  → Committed %d updates (total: %d)", len(batch), stats.rows_updated)
        except Exception as exc:
            write_conn.rollback()
            msg = f"Batch commit failed: {exc}"
            log.error(msg)
            stats.errors += 1
            stats.error_details.append(msg)
        batch.clear()

    log.info("Streaming DrugMasterLinkage, matching against %d NULL rows …", stats.null_rows_loaded)

    for raw_row in src_cur:
        stats.source_records_read += 1
        jsonb             = raw_row["combined_clean_jsonb"]
        master_linkage_id = raw_row["master_linkage_id"]

        if not jsonb:
            continue

        rxnorm_list = jsonb.get("rxnorm")
        if not isinstance(rxnorm_list, list) or not rxnorm_list:
            continue

        for entry in rxnorm_list:
            if not isinstance(entry, dict):
                continue

            raw_formulation = (entry.get("generic_formulation") or "").strip()
            dosage_form     = (entry.get("specific_dosage_form") or "").strip()
            rxcui_val       = (entry.get("rxcui") or "").strip() or None

            if not raw_formulation:
                continue

            stats.rxnorm_entries_checked += 1

            # Match against the NULL-row lookup using the UNCLEANED formulation
            key = (master_linkage_id, raw_formulation, dosage_form)
            formulation_id = lookup.get(key)

            if not formulation_id:
                continue

            stats.successful_matches += 1

            if verbose:
                log.debug(
                    "MATCH | formulation_id=%s | rxcui=%-12s | raw_form=%.60s",
                    formulation_id, rxcui_val, raw_formulation,
                )

            if dry_run:
                stats.rows_updated += 1
            else:
                batch.append((rxcui_val, raw_formulation, formulation_id))
                stats.rows_updated += 1

            if not dry_run and len(batch) >= batch_size:
                flush_batch()

        if stats.source_records_read % 5000 == 0:
            log.info(
                "Progress: %d records | %d matches found | %d updated | %d errors",
                stats.source_records_read, stats.successful_matches,
                stats.rows_updated, stats.errors,
            )

    if batch:
        flush_batch()

    src_cur.close()
    write_cur.close()


def run_verification(conn, log: logging.Logger):
    log.info("Running post-fix verification …")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(rxcui) AS has_rxcui,
                COUNT(*) - COUNT(rxcui) AS still_missing_rxcui
            FROM drugdb.drug
        """)
        row = cur.fetchone()
        print("\n" + "=" * 60)
        print("  Post-Fix Verification")
        print("=" * 60)
        print(f"  Total rows    : {row[0]:>10,}")
        print(f"  Has rxcui     : {row[1]:>10,}")
        print(f"  Still NULL    : {row[2]:>10,}")
        print("=" * 60)
        if row[2] > 0:
            cur.execute("""
                SELECT formulation_id, generic_name, generic_formulation,
                       dosage_forms, generic_formulation_original
                FROM drugdb.drug
                WHERE rxcui IS NULL
                LIMIT 10
            """)
            print("\n  Still-NULL rows (first 10):")
            for r in cur.fetchall():
                print(f"    {r[0]} | {r[1]!r:.25s} | orig={r[4]!r:.50s}")
        else:
            print("\n  All rows have rxcui populated.")


REPORT_TEMPLATE = """
============================================================
  Fix: Uncleaned-Row RxNorm Patch Summary  ({timestamp})
  {dry_run_banner}============================================================
  NULL rows loaded for patching             : {null_rows_loaded:>10,}
  Source records streamed                   : {source_records_read:>10,}
  RxNorm entries checked                    : {rxnorm_entries_checked:>10,}
  Successful matches                        : {successful_matches:>10,}
  Rows updated                              : {rows_updated:>10,}
  Errors                                    : {errors:>10,}
============================================================
"""


def print_report(stats: Stats, dry_run: bool):
    banner = "*** DRY-RUN — NO DATA WRITTEN ***\n  " if dry_run else ""
    print(REPORT_TEMPLATE.format(
        timestamp             = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        dry_run_banner        = banner,
        null_rows_loaded      = stats.null_rows_loaded,
        source_records_read   = stats.source_records_read,
        rxnorm_entries_checked= stats.rxnorm_entries_checked,
        successful_matches    = stats.successful_matches,
        rows_updated          = stats.rows_updated,
        errors                = stats.errors,
    ))
    if stats.error_details:
        for d in stats.error_details[:20]:
            print(f"  {d}")


def build_parser():
    p = argparse.ArgumentParser(
        description="Patch 17,128 drug rows left NULL by UUID mismatch in update_drug_rxnorm_columns.py"
    )
    p.add_argument("--host",       default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--dbname",     default="postgres")
    p.add_argument("--user",       default="postgres")
    p.add_argument("--password",   required=True)
    p.add_argument("--port",       type=int, default=5432)
    p.add_argument("--dry-run",    action="store_true")
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--log-file",   default=None)
    p.add_argument("--verbose",    action="store_true")
    p.add_argument("--verify",     action="store_true")
    return p


def setup_logging(verbose: bool, log_file: Optional[str]) -> logging.Logger:
    log = logging.getLogger("rxnorm_fix")
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        log.addHandler(fh)
    return log


def connect(args, log):
    try:
        return psycopg2.connect(
            host=args.host, dbname=args.dbname, user=args.user,
            password=args.password, port=args.port, connect_timeout=30,
        )
    except Exception as exc:
        log.error("Connection failed: %s", exc)
        sys.exit(1)


def main():
    args  = build_parser().parse_args()
    log   = setup_logging(args.verbose, args.log_file)
    stats = Stats()

    if args.dry_run:
        log.info("*** DRY-RUN MODE — no changes will be written ***")

    read_conn  = connect(args, log)
    write_conn = connect(args, log)
    read_conn.autocommit = False

    try:
        lookup = build_lookup(write_conn, log, stats)
        if not lookup:
            log.info("No NULL rows found — nothing to patch. Exiting.")
            return

        run_fix(read_conn, write_conn, lookup,
                dry_run=args.dry_run, batch_size=args.batch_size,
                verbose=args.verbose, log=log, stats=stats)

        if args.verify and not args.dry_run:
            run_verification(write_conn, log)

    except KeyboardInterrupt:
        log.warning("Interrupted — rolling back.")
        write_conn.rollback()
    except Exception as exc:
        log.error("Fatal: %s", exc, exc_info=True)
        write_conn.rollback()
    finally:
        read_conn.close()
        write_conn.close()

    print_report(stats, args.dry_run)


if __name__ == "__main__":
    main()
