#!/usr/bin/env python3
"""
update_drug_rxnorm_columns.py

Adds rxnorm_generic_formulation and rxcui columns to drugdb.drug (if not already
present) and populates them from DrugMasterLinkage.combined_clean_jsonb.

Matching strategy: the formulation_id in drugdb.drug is a deterministic UUID5
derived from (master_linkage_id, cleaned_generic_formulation, dosage_form) —
the same seed formula used by populate_drug_table.py.  This script recomputes
that UUID for every rxnorm entry, so no in-memory lookup dict is needed and
matching is exact and O(1) per row.

Usage:
  python3 update_drug_rxnorm_columns.py --password <pwd>
  python3 update_drug_rxnorm_columns.py --password <pwd> --dry-run --verbose
  python3 update_drug_rxnorm_columns.py --password <pwd> --batch-size 500 --log-file logs/rxnorm_update.log
  python3 update_drug_rxnorm_columns.py --password <pwd> --limit 100 --dry-run --verbose
"""

import argparse
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras


# ──────────────────────────────────────────────────────────────
# DDL
# ──────────────────────────────────────────────────────────────

DDL_SQL = """
ALTER TABLE drugdb.drug
    ADD COLUMN IF NOT EXISTS rxnorm_generic_formulation TEXT,
    ADD COLUMN IF NOT EXISTS rxcui                      VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_drug_rxnorm_formulation
    ON drugdb.drug (rxnorm_generic_formulation);

CREATE INDEX IF NOT EXISTS idx_drug_rxcui
    ON drugdb.drug (rxcui);
"""


def ensure_columns_and_indexes(conn, dry_run: bool, log: logging.Logger):
    with conn.cursor() as cur:
        for col in ("rxnorm_generic_formulation", "rxcui"):
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name   = 'drug'
                  AND column_name  = %s
            """, (col,))
            if cur.fetchone():
                log.info("Column '%s' already exists.", col)
            else:
                log.info("Column '%s' missing — adding …", col)
                if not dry_run:
                    col_type = "TEXT" if col == "rxnorm_generic_formulation" else "VARCHAR(50)"
                    cur.execute(
                        f"ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS {col} {col_type};"
                    )
                    conn.commit()

        for idx, col in [
            ("idx_drug_rxnorm_formulation", "rxnorm_generic_formulation"),
            ("idx_drug_rxcui",              "rxcui"),
        ]:
            cur.execute(
                "SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='drug' AND indexname=%s",
                (idx,),
            )
            if cur.fetchone():
                log.info("Index '%s' already exists.", idx)
            else:
                log.info("Index '%s' missing — creating …", idx)
                if not dry_run:
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {idx} ON drugdb.drug ({col});"
                    )
                    conn.commit()


# ──────────────────────────────────────────────────────────────
# Dosage-form suffix stripping (identical to populate_drug_table.py)
# ──────────────────────────────────────────────────────────────

_DOSAGE_FORM_SUFFIX_MAP: dict[str, list[str]] = {
    "TABLET, EXTENDED RELEASE":                   ["Extended Release Oral Tablet"],
    "TABLET, DELAYED RELEASE (OBS 06-25-01)":     ["Delayed Release Oral Tablet"],
    "GASTRO-RESISTANT TABLET":                    ["Delayed Release Oral Tablet"],
    "CAPSULE, EXTENDED RELEASE":                  ["Extended Release Oral Capsule"],
    "TABLET,DISINTEGRATING":                      ["Disintegrating Oral Tablet"],
    "TABLET, SUBLINGUAL":                         ["Sublingual Tablet"],
    "TABLET,CHEWABLE":                            ["Chewable Tablet"],
    "TABLET, BUCCAL":                             ["Buccal Tablet"],
    "INHALATION GAS":                             ["Gas for Inhalation"],
    "INHALATION SOLUTION":                        ["Inhalation Solution"],
    "INHALATION SUSPENSION":                      ["Inhalation Suspension"],
    "INHALATION POWDER":                          ["Inhalation Powder"],
    "SOLUTION FOR INJECTION":                     ["Injectable Solution"],
    "SUSPENSION FOR INJECTION":                   ["Injectable Suspension"],
    "SUSPENSION, ORAL (FINAL DOSE FORM)":         ["Oral Suspension"],
    "SOLUTION, ORAL":                             ["Oral Solution"],
    "GRANULES FOR ORAL SUSPENSION":               ["Granules for Oral Suspension"],
    "GRANULES FOR ORAL SOLUTION":                 ["Granules for Oral Solution"],
    "POWDER FOR ORAL SUSPENSION":                 ["Powder for Oral Suspension"],
    "POWDER FOR ORAL SOLUTION":                   ["Powder for Oral Solution"],
    "SUSPENSION,EXTENDED RELEASE VIAL (ML)":      ["Extended Release Suspension"],
    "ORAL POWDER":                                ["Oral Powder"],
    "ORAL GEL":                                   ["Oral Gel"],
    "CUTANEOUS SOLUTION":                         ["Topical Solution"],
    "CUTANEOUS FOAM":                             ["Topical Foam"],
    "CUTANEOUS POWDER":                           ["Topical Powder"],
    "EYE OINTMENT":                               ["Ophthalmic Ointment"],
    "EYE GEL":                                    ["Ophthalmic Gel"],
    "SUPPOSITORY, RECTAL":                        ["Rectal Suppository"],
    "RECTAL CREAM":                               ["Rectal Cream"],
    "RECTAL GEL":                                 ["Rectal Gel"],
    "RECTAL FOAM":                                ["Rectal Foam"],
    "RECTAL OINTMENT":                            ["Rectal Ointment"],
    "VAGINAL CREAM":                              ["Vaginal Cream"],
    "VAGINAL GEL":                                ["Vaginal Gel"],
    "RING, VAGINAL":                              ["Vaginal System"],
    "MOUTHWASH":                                  ["Mouthwash"],
    "GARGLE":                                     ["Mouthwash"],
    "LOZENGE":                                    ["Oral Lozenge"],
    "TROCHE":                                     ["Oral Lozenge"],
    "SOLUTION, IRRIGATION":                       ["Irrigation Solution"],
    "TRANSDERMAL PATCH":                          ["Transdermal System"],
    "AUTO-INJECTOR (EA)":                         ["Auto-Injector"],
    "PELLET (EA)":                                ["Oral Pellet"],
    "ENEMA (EA)":                                 ["Enema"],
    "ENEMA (ML)":                                 ["Enema"],
    "NASAL GEL":                                  ["Nasal Gel"],
    "NASAL POWDER":                               ["Nasal Powder"],
    "TAPE, MEDICATED":                            ["Medicated Tape"],
    "TOOTHPASTE":                                 ["Toothpaste"],
}

_COMPILED_PATTERNS: list[tuple[str, list[re.Pattern]]] = []
for _df_key, _suffixes in _DOSAGE_FORM_SUFFIX_MAP.items():
    _pats = []
    for _sfx in _suffixes:
        _escaped = re.escape(_sfx).replace(r'\ ', r'\s+')
        _pats.append(re.compile(r'\s+' + _escaped + r'\s*$', re.IGNORECASE))
    _COMPILED_PATTERNS.append((_df_key, _pats))


def strip_dosage_form_suffix(generic_formulation: str, dosage_form: str) -> tuple[str, str]:
    """
    Returns (cleaned_formulation, stage) where stage is 'stage1', 'stage2', or 'none'.
    Identical transformation to populate_drug_table.py — must stay in sync.
    """
    gf = (generic_formulation or "").strip()
    df = (dosage_form or "").strip()
    if not gf:
        return gf, "none"

    # Stage 1: exact (case-insensitive) suffix match
    if df and gf.lower().endswith(df.lower()):
        return gf[: -len(df)].strip(), "stage1"

    # Stage 2: mapped suffix via compiled regex
    df_upper = df.upper()
    for df_key, patterns in _COMPILED_PATTERNS:
        if df_upper == df_key.upper():
            for pat in patterns:
                cleaned = pat.sub('', gf).strip()
                if cleaned != gf:
                    return cleaned, "stage2"
            break

    return gf, "none"


# ──────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────

@dataclass
class Stats:
    source_records_read:          int = 0
    source_records_skipped:       int = 0
    rxnorm_entries_found:         int = 0
    rxnorm_entries_with_rxcui:    int = 0
    rxnorm_entries_without_rxcui: int = 0
    successful_matches:           int = 0
    failed_matches:               int = 0
    rows_updated:                 int = 0
    rows_skipped_already_set:     int = 0
    errors:                       int = 0
    stage1_direct_match:          int = 0
    stage2_regex_match:           int = 0
    cleaning_no_change:           int = 0
    error_details: list = field(default_factory=list)
    failed_match_details: list = field(default_factory=list)


# ──────────────────────────────────────────────────────────────
# ETL core
# ──────────────────────────────────────────────────────────────

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
      AND  (rxcui IS NULL OR rxnorm_generic_formulation IS NULL)
"""


def run_etl(
    read_conn,
    write_conn,
    dry_run: bool,
    batch_size: int,
    limit: Optional[int],
    verbose: bool,
    log: logging.Logger,
    stats: Stats,
):
    # Named server-side cursor for streaming — read_conn must never commit
    src_cur = read_conn.cursor(
        name="rxnorm_update_stream",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    src_cur.itersize = 500

    query = FETCH_SQL
    if limit:
        query += f" LIMIT {limit}"
    src_cur.execute(query)

    write_cur = write_conn.cursor()
    batch: list[tuple] = []   # (rxcui, rxnorm_generic_formulation, formulation_id)

    def flush_batch():
        if not batch or dry_run:
            batch.clear()
            return
        try:
            write_cur.executemany(UPDATE_SQL, batch)
            write_conn.commit()
            log.info("  → Committed %d updates (total updated so far: %d)",
                     len(batch), stats.rows_updated)
        except Exception as exc:
            write_conn.rollback()
            msg = f"Batch commit failed: {exc}"
            log.error(msg)
            stats.errors += 1
            stats.error_details.append(msg)
        batch.clear()

    log.info("Streaming DrugMasterLinkage records …")

    for raw_row in src_cur:
        stats.source_records_read += 1
        jsonb             = raw_row["combined_clean_jsonb"]
        master_linkage_id = raw_row["master_linkage_id"]

        if not jsonb:
            stats.source_records_skipped += 1
            continue

        rxnorm_list = jsonb.get("rxnorm")
        if not isinstance(rxnorm_list, list) or not rxnorm_list:
            stats.source_records_skipped += 1
            continue

        for entry in rxnorm_list:
            if not isinstance(entry, dict):
                continue

            stats.rxnorm_entries_found += 1

            raw_formulation = (entry.get("generic_formulation") or "").strip()
            dosage_form     = (entry.get("specific_dosage_form") or "").strip()
            rxcui_val       = (entry.get("rxcui") or "").strip() or None

            if not raw_formulation:
                continue

            if rxcui_val:
                stats.rxnorm_entries_with_rxcui += 1
            else:
                stats.rxnorm_entries_without_rxcui += 1

            # Reproduce the exact cleaning used by populate_drug_table.py
            cleaned_formulation, stage = strip_dosage_form_suffix(raw_formulation, dosage_form)
            if stage == "stage1":
                stats.stage1_direct_match += 1
            elif stage == "stage2":
                stats.stage2_regex_match += 1
            else:
                stats.cleaning_no_change += 1

            # Recompute the deterministic UUID — same formula as populate_drug_table.py
            seed           = f"{master_linkage_id}|{cleaned_formulation}|{dosage_form}"
            formulation_id = str(uuid.uuid5(uuid.NAMESPACE_OID, seed))

            if verbose and dry_run:
                log.debug(
                    "DRY-RUN | formulation_id=%s | rxcui=%-12s | stage=%-7s | "
                    "raw_form=%.50s",
                    formulation_id, rxcui_val, stage, raw_formulation,
                )

            stats.successful_matches += 1

            if dry_run:
                stats.rows_updated += 1
            else:
                batch.append((rxcui_val, raw_formulation, formulation_id))
                stats.rows_updated += 1

            if not dry_run and len(batch) >= batch_size:
                flush_batch()

        if stats.source_records_read % 500 == 0:
            log.info(
                "Progress: %d records | %d rxnorm entries | %d updates queued | %d errors",
                stats.source_records_read,
                stats.rxnorm_entries_found,
                stats.rows_updated,
                stats.errors,
            )

    if batch:
        flush_batch()

    src_cur.close()
    write_cur.close()


# ──────────────────────────────────────────────────────────────
# Verification queries
# ──────────────────────────────────────────────────────────────

def run_verification(conn, log: logging.Logger):
    log.info("Running post-population verification …")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*)                            AS total_rows,
                COUNT(rxnorm_generic_formulation)   AS has_rxnorm_formulation,
                COUNT(rxcui)                        AS has_rxcui,
                COUNT(*) - COUNT(rxnorm_generic_formulation) AS missing_rxnorm_formulation,
                COUNT(*) - COUNT(rxcui)             AS missing_rxcui
            FROM drugdb.drug
        """)
        row = cur.fetchone()
        print("\n" + "=" * 60)
        print("  Verification Summary")
        print("=" * 60)
        print(f"  Total rows                     : {row[0]:>10,}")
        print(f"  Has rxnorm_generic_formulation : {row[1]:>10,}")
        print(f"  Has rxcui                      : {row[2]:>10,}")
        print(f"  Missing rxnorm_generic_formulation: {row[3]:>7,}")
        print(f"  Missing rxcui                  : {row[4]:>10,}")
        print("=" * 60)

        print("\n  Sample populated rows (first 10):")
        cur.execute("""
            SELECT
                generic_name,
                generic_formulation,
                rxnorm_generic_formulation,
                rxcui,
                dosage_forms
            FROM drugdb.drug
            WHERE rxnorm_generic_formulation IS NOT NULL
            LIMIT 10
        """)
        for i, r in enumerate(cur.fetchall(), 1):
            print(f"  {i:2d}. generic_name={r[0]!r:.30s} | "
                  f"cleaned={r[1]!r:.30s} | "
                  f"original={r[2]!r:.45s} | "
                  f"rxcui={r[3]!r}")

        print("\n  Rows with NULL rxnorm columns (first 10):")
        cur.execute("""
            SELECT formulation_id, master_linkage_id, generic_name,
                   generic_formulation, dosage_forms
            FROM drugdb.drug
            WHERE rxnorm_generic_formulation IS NULL
               OR rxcui IS NULL
            LIMIT 10
        """)
        nulls = cur.fetchall()
        if nulls:
            for r in nulls:
                print(f"    formulation_id={r[0]} | generic_name={r[2]!r:.30s} | "
                      f"dosage_forms={r[4]!r:.30s}")
        else:
            print("    None — all rows have both new columns populated.")


# ──────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────

REPORT_TEMPLATE = """
============================================================
  Drug Table RxNorm Column Update Summary  ({timestamp})
  {dry_run_banner}============================================================
  Source records processed                  : {source_records_read:>10,}
  Source records skipped (no rxnorm/jsonb)  : {source_records_skipped:>10,}

  RxNorm entries found                      : {rxnorm_entries_found:>10,}
  RxNorm entries WITH rxcui                 : {rxnorm_with_rxcui:>10,}
  RxNorm entries WITHOUT rxcui              : {rxnorm_without_rxcui:>10,}

  Formulation cleaning breakdown:
    Stage 1 (direct suffix match)           : {stage1:>10,}
    Stage 2 (regex mapped suffix)           : {stage2:>10,}
    No change (form suffix not found)       : {no_change:>10,}

  Successful formulation_id matches         : {successful_matches:>10,}
  Failed matches                            : {failed_matches:>10,}
  Rows updated (or would be updated)        : {rows_updated:>10,}
  Errors                                    : {errors:>10,}
============================================================
"""


def print_report(stats: Stats, dry_run: bool):
    banner = "*** DRY-RUN — NO DATA WRITTEN ***\n  " if dry_run else ""
    print(REPORT_TEMPLATE.format(
        timestamp             = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        dry_run_banner        = banner,
        source_records_read   = stats.source_records_read,
        source_records_skipped= stats.source_records_skipped,
        rxnorm_entries_found  = stats.rxnorm_entries_found,
        rxnorm_with_rxcui     = stats.rxnorm_entries_with_rxcui,
        rxnorm_without_rxcui  = stats.rxnorm_entries_without_rxcui,
        stage1                = stats.stage1_direct_match,
        stage2                = stats.stage2_regex_match,
        no_change             = stats.cleaning_no_change,
        successful_matches    = stats.successful_matches,
        failed_matches        = stats.failed_matches,
        rows_updated          = stats.rows_updated,
        errors                = stats.errors,
    ))
    if stats.error_details:
        print("Error details (first 20):")
        for d in stats.error_details[:20]:
            print(f"  {d}")
        if len(stats.error_details) > 20:
            print(f"  … and {len(stats.error_details) - 20} more — check --log-file for full list")

    if stats.failed_match_details:
        print(f"\nFailed matches (first 20 of {len(stats.failed_match_details)}):")
        for d in stats.failed_match_details[:20]:
            print(f"  rxcui={d['rxcui']!r} | dosage_form={d['dosage_form']!r} | "
                  f"formulation={d['raw_formulation']!r:.60s}")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        description="Add and populate rxnorm_generic_formulation + rxcui in drugdb.drug"
    )
    p.add_argument("--host",       default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--dbname",     default="postgres")
    p.add_argument("--user",       default="postgres")
    p.add_argument("--password",   required=True, help="PostgreSQL password")
    p.add_argument("--port",       type=int, default=5432)
    p.add_argument("--dry-run",    action="store_true",
                   help="Show what would be updated without writing anything")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Commit every N rows (default: 1000)")
    p.add_argument("--limit",      type=int, default=None,
                   help="Process only the first N DrugMasterLinkage records (for testing)")
    p.add_argument("--log-file",   help="Write detailed log to this file path")
    p.add_argument("--verbose",    action="store_true",
                   help="Print DEBUG-level messages to console")
    p.add_argument("--skip-ddl",   action="store_true",
                   help="Skip ALTER TABLE / CREATE INDEX (columns already exist)")
    p.add_argument("--verify",     action="store_true",
                   help="Run verification queries after population")
    return p


def setup_logging(verbose: bool, log_file: Optional[str]) -> logging.Logger:
    log = logging.getLogger("rxnorm_update")
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    log.addHandler(console)
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        log.addHandler(fh)
    return log


def connect(args, log: logging.Logger):
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
    if args.limit:
        log.info("*** LIMIT=%d — processing first %d source records only ***",
                 args.limit, args.limit)

    log.info("Connecting to %s:%s/%s as %s", args.host, args.port, args.dbname, args.user)
    read_conn  = connect(args, log)
    write_conn = connect(args, log)

    # read_conn must never commit — named server-side cursor depends on open transaction
    read_conn.autocommit = False

    try:
        if not args.skip_ddl:
            log.info("Phase 1: Ensuring columns and indexes exist …")
            ensure_columns_and_indexes(write_conn, args.dry_run, log)

        log.info("Phase 2: Populating rxnorm columns …")
        run_etl(
            read_conn, write_conn,
            dry_run    = args.dry_run,
            batch_size = args.batch_size,
            limit      = args.limit,
            verbose    = args.verbose,
            log        = log,
            stats      = stats,
        )

        if args.verify and not args.dry_run:
            run_verification(write_conn, log)

    except KeyboardInterrupt:
        log.warning("Interrupted by user — rolling back any open transaction.")
        write_conn.rollback()
    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=True)
        write_conn.rollback()
    finally:
        read_conn.close()
        write_conn.close()

    print_report(stats, args.dry_run)


if __name__ == "__main__":
    main()
