#!/usr/bin/env python3
"""
populate_drug_table.py

Reads public.DrugMasterLinkage.combined_clean_jsonb and inserts one row into
drugdb.drug for every rxnorm entry found.

Steps performed automatically:
  1. Create drugdb.drug if it does not exist.
  2. Add generic_formulation / dosage_forms / master_linkage_id columns if missing.
  3. Stream all DrugMasterLinkage rows and extract rxnorm entries.
  4. Transform + insert into drug (batch commits for performance).

Usage:
  python3 populate_drug_table.py --password <pwd>
  python3 populate_drug_table.py --password <pwd> --dry-run --verbose
  python3 populate_drug_table.py --password <pwd> --batch-size 500 --log-file logs/drug.log
"""

import argparse
import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras


# ──────────────────────────────────────────────────────────────
# DDL helpers
# ──────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS drugdb.drug (
    formulation_id      UUID  PRIMARY KEY,
    generic_name        TEXT,
    generic_formulation TEXT,
    dosage_forms        TEXT,
    master_linkage_id   UUID
);
"""

# Each entry: (column_name, definition) — checked/added if the table already exists
REQUIRED_COLUMNS = [
    ("generic_formulation", "TEXT"),
    ("dosage_forms",        "TEXT"),
    ("master_linkage_id",   "UUID"),
]


def ensure_schema(conn, dry_run: bool, log: logging.Logger):
    """Create table and add any missing columns."""
    with conn.cursor() as cur:

        # 1. Create table if needed
        log.info("Ensuring drugdb.drug table exists …")
        if not dry_run:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()

        # 2. For each required column, add it if missing
        for col_name, col_def in REQUIRED_COLUMNS:
            cur.execute("""
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name   = 'drug'
                  AND column_name  = %s
            """, (col_name,))
            exists = cur.fetchone() is not None

            if not exists:
                log.info("Column '%s' missing — adding it (type %s) …", col_name, col_def)
                if not dry_run:
                    cur.execute(
                        f"ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS {col_name} {col_def};"
                    )
                    conn.commit()
            else:
                log.info("Column '%s' already present.", col_name)


# ──────────────────────────────────────────────────────────────
# Data structures
# ──────────────────────────────────────────────────────────────

@dataclass
class DrugRow:
    formulation_id:     str
    generic_name:       str
    generic_formulation: str
    dosage_forms:       str
    master_linkage_id:  str   # UUID from DrugMasterLinkage.master_linkage_id


@dataclass
class Stats:
    source_rows_read:       int = 0
    source_rows_skipped:    int = 0   # null jsonb or no rxnorm entries
    rxnorm_entries_found:   int = 0
    rows_inserted:          int = 0
    errors:                 int = 0
    error_details: list = field(default_factory=list)


# ──────────────────────────────────────────────────────────────
# Transform helpers
# ──────────────────────────────────────────────────────────────

# Maps EU/uppercase dosage_forms codes → the RxNorm suffix that appears in
# generic_formulation for that code. Built from Phase 2–3 analysis (2026-05-02).
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

# Pre-compiled regex patterns: (dosage_form_key, compiled_regex) pairs.
# Each pattern strips the known RxNorm suffix from the end of generic_formulation.
import re as _re

_COMPILED_PATTERNS: list[tuple[str, list[_re.Pattern]]] = []
for _df_key, _suffixes in _DOSAGE_FORM_SUFFIX_MAP.items():
    _patterns = []
    for _sfx in _suffixes:
        # e.g. "Extended Release Oral Tablet" → r'\s+Extended\s+Release\s+Oral\s+Tablet\s*$'
        _escaped = _re.escape(_sfx).replace(r'\ ', r'\s+')
        _patterns.append(_re.compile(r'\s+' + _escaped + r'\s*$', _re.IGNORECASE))
    _COMPILED_PATTERNS.append((_df_key, _patterns))


def strip_dosage_form_suffix(generic_formulation: str, dosage_form: str) -> str:
    """
    Remove the RxNorm dosage-form phrase from the tail of generic_formulation.

    Handles two cases:
    1. Direct match: generic_formulation ends with the dosage_form string verbatim.
       "quetiapine 100 MG Oral Tablet", "Oral Tablet"  →  "quetiapine 100 MG"

    2. Mapped match: dosage_form is a EU/uppercase code whose RxNorm equivalent
       suffix is embedded in generic_formulation.
       "24 HR metformin 1000 MG Extended Release Oral Tablet", "TABLET, EXTENDED RELEASE"
       →  "24 HR metformin 1000 MG"
    """
    gf = (generic_formulation or "").strip()
    df = (dosage_form or "").strip()
    if not gf:
        return gf

    # Case 1: exact suffix match (handles human-readable form names)
    if df and gf.lower().endswith(df.lower()):
        return gf[: -len(df)].strip()

    # Case 2: mapped suffix via compiled regex patterns
    df_upper = df.upper()
    for df_key, patterns in _COMPILED_PATTERNS:
        if df_upper == df_key.upper():
            for pat in patterns:
                cleaned = pat.sub('', gf).strip()
                if cleaned != gf:
                    return cleaned
            break  # key matched but no pattern fired — return as-is

    return gf


def extract_rows_from_record(jsonb: dict, master_linkage_id: str) -> list[DrugRow]:
    """
    Convert one DrugMasterLinkage JSONB record into a list of DrugRow objects
    (one per rxnorm entry).  master_linkage_id is the UUID PK of that source row.
    """
    rows: list[DrugRow] = []

    # ── generic_name ────────────────────────────────────────
    openfda      = jsonb.get("openfda") or {}
    drug_info    = openfda.get("drug_info") or {}
    generic_name = (drug_info.get("generic_name") or "").strip()

    # ── rxnorm entries ──────────────────────────────────────
    rxnorm_list = jsonb.get("rxnorm")
    if not isinstance(rxnorm_list, list) or not rxnorm_list:
        return rows  # nothing to do

    for entry in rxnorm_list:
        if not isinstance(entry, dict):
            continue

        raw_formulation = (entry.get("generic_formulation") or "").strip()
        dosage_form     = (entry.get("specific_dosage_form") or "").strip()

        # Skip entries that have no formulation text
        if not raw_formulation:
            continue

        generic_formulation = strip_dosage_form_suffix(raw_formulation, dosage_form)

        # Deterministic UUID from content so re-runs are idempotent:
        # same (master_linkage_id, generic_formulation, dosage_form) → same UUID.
        seed = f"{master_linkage_id}|{generic_formulation}|{dosage_form}"
        formulation_id = str(uuid.uuid5(uuid.NAMESPACE_OID, seed))

        rows.append(DrugRow(
            formulation_id      = formulation_id,
            generic_name        = generic_name,
            generic_formulation = generic_formulation,
            dosage_forms        = dosage_form,
            master_linkage_id   = master_linkage_id,
        ))

    return rows


# ──────────────────────────────────────────────────────────────
# ETL core
# ──────────────────────────────────────────────────────────────

FETCH_SQL = """
    SELECT master_linkage_id::text, combined_clean_jsonb
    FROM   public."DrugMasterLinkage"
    WHERE  combined_clean_jsonb IS NOT NULL
"""

INSERT_SQL = """
    INSERT INTO drugdb.drug
        (formulation_id, generic_name, generic_formulation, dosage_forms, master_linkage_id)
    VALUES (%s, %s, %s, %s, %s::uuid)
    ON CONFLICT (formulation_id) DO NOTHING
"""


def run_etl(read_conn, write_conn, dry_run: bool, batch_size: int, log: logging.Logger, stats: Stats):

    # read_conn uses autocommit=True so the server-side cursor stays open
    # across write_conn commits (named cursors are transaction-scoped).
    src_cur = read_conn.cursor(name="dml_stream", cursor_factory=psycopg2.extras.RealDictCursor)
    src_cur.itersize = 500
    src_cur.execute(FETCH_SQL)

    write_cur = write_conn.cursor()
    batch: list[tuple] = []

    def flush_batch():
        if not batch or dry_run:
            batch.clear()
            return
        try:
            write_cur.executemany(INSERT_SQL, batch)
            write_conn.commit()
            log.info("  → Committed %d rows (total inserted so far: %d)",
                     len(batch), stats.rows_inserted)
        except Exception as exc:
            write_conn.rollback()
            msg = f"Batch commit failed: {exc}"
            log.error(msg)
            stats.errors += 1
            stats.error_details.append(msg)
        batch.clear()

    log.info("Streaming DrugMasterLinkage records …")

    for raw_row in src_cur:
        stats.source_rows_read += 1
        jsonb             = raw_row["combined_clean_jsonb"]
        master_linkage_id = raw_row["master_linkage_id"]

        if not jsonb:
            stats.source_rows_skipped += 1
            continue

        # Extract rows from this record
        try:
            drug_rows = extract_rows_from_record(jsonb, master_linkage_id)
        except Exception as exc:
            msg = f"Record {stats.source_rows_read} (master_linkage_id={master_linkage_id}): extraction error — {exc}"
            log.error(msg)
            stats.errors += 1
            stats.error_details.append(msg)
            continue

        if not drug_rows:
            stats.source_rows_skipped += 1
            continue

        stats.rxnorm_entries_found += len(drug_rows)

        for dr in drug_rows:
            if dry_run:
                log.debug(
                    "DRY-RUN INSERT | master_linkage_id=%s | generic_name=%-25s | generic_formulation=%-45s | dosage_forms=%s",
                    dr.master_linkage_id, repr(dr.generic_name),
                    repr(dr.generic_formulation), repr(dr.dosage_forms),
                )
                stats.rows_inserted += 1
            else:
                batch.append((
                    dr.formulation_id,
                    dr.generic_name,
                    dr.generic_formulation,
                    dr.dosage_forms,
                    dr.master_linkage_id,
                ))
                stats.rows_inserted += 1

        # Commit batch when it reaches batch_size
        if not dry_run and len(batch) >= batch_size:
            flush_batch()

        # Progress report every 100 source records
        if stats.source_rows_read % 100 == 0:
            log.info(
                "Progress: %d records read | %d rxnorm entries | %d rows inserted | %d errors",
                stats.source_rows_read,
                stats.rxnorm_entries_found,
                stats.rows_inserted,
                stats.errors,
            )

    # Flush any remaining rows
    if batch:
        flush_batch()

    src_cur.close()
    write_cur.close()


# ──────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────

REPORT_TEMPLATE = """
============================================================
  Drug Table Population Summary  ({timestamp})
  {dry_run_banner}============================================================
  Total DrugMasterLinkage records processed : {source_rows_read:>10,}
  Records skipped (no rxnorm / null jsonb)  : {source_rows_skipped:>10,}

  RxNorm entries found                      : {rxnorm_entries:>10,}
  Total drug table rows inserted            : {rows_inserted:>10,}
  Total errors                              : {errors:>10,}
============================================================

Verification queries (run against the 'postgres' database):

  SELECT COUNT(*) FROM drugdb.drug;

  SELECT generic_name, COUNT(*) AS formulations
  FROM   drugdb.drug
  GROUP  BY generic_name
  ORDER  BY formulations DESC
  LIMIT  20;

  -- Check master_linkage_id is populated
  SELECT formulation_id, generic_name, generic_formulation, dosage_forms, master_linkage_id
  FROM   drugdb.drug
  LIMIT  10;

  -- Verify linkage back to source
  SELECT d.generic_name, d.generic_formulation, d.dosage_forms, m.master_linkage_id
  FROM   drugdb.drug d
  JOIN   public."DrugMasterLinkage" m ON m.master_linkage_id = d.master_linkage_id
  LIMIT  10;
"""


def print_report(stats: Stats, dry_run: bool):
    banner = "*** DRY-RUN — NO DATA WRITTEN ***\n  " if dry_run else ""
    print(REPORT_TEMPLATE.format(
        timestamp        = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        dry_run_banner   = banner,
        source_rows_read = stats.source_rows_read,
        source_rows_skipped = stats.source_rows_skipped,
        rxnorm_entries   = stats.rxnorm_entries_found,
        rows_inserted    = stats.rows_inserted,
        errors           = stats.errors,
    ))
    if stats.error_details:
        print("Error details (first 20):")
        for detail in stats.error_details[:20]:
            print(f"  {detail}")
        if len(stats.error_details) > 20:
            print(f"  … and {len(stats.error_details) - 20} more — check --log-file for full list")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        description="Populate drugdb.drug from public.DrugMasterLinkage (postgres database)"
    )
    p.add_argument("--host",       default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--dbname",     default="postgres")
    p.add_argument("--user",       default="postgres")
    p.add_argument("--password",   required=True, help="PostgreSQL password")
    p.add_argument("--port",       type=int, default=5432)
    p.add_argument("--dry-run",    action="store_true",
                   help="Show what would be inserted without writing anything")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Commit every N rows (default: 1000)")
    p.add_argument("--log-file",   help="Write detailed log to this file path")
    p.add_argument("--verbose",    action="store_true",
                   help="Print DEBUG-level messages to console")
    return p


def setup_logging(verbose: bool, log_file: Optional[str]) -> logging.Logger:
    log = logging.getLogger("drug_populate")
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


def main():
    args   = build_parser().parse_args()
    log    = setup_logging(args.verbose, args.log_file)
    stats  = Stats()

    if args.dry_run:
        log.info("*** DRY-RUN MODE — no changes will be written ***")

    log.info("Connecting to %s:%s/%s as %s", args.host, args.port, args.dbname, args.user)
    try:
        conn = psycopg2.connect(
            host=args.host, dbname=args.dbname, user=args.user,
            password=args.password, port=args.port, connect_timeout=30,
        )
    except Exception as exc:
        log.error("Connection failed: %s", exc)
        sys.exit(1)

    # Two connections: read_conn streams source data (autocommit keeps the
    # server-side cursor alive); write_conn handles batch inserts with commits.
    try:
        write_conn = psycopg2.connect(
            host=args.host, dbname=args.dbname, user=args.user,
            password=args.password, port=args.port, connect_timeout=30,
        )
    except Exception as exc:
        log.error("Write connection failed: %s", exc)
        conn.close()
        sys.exit(1)

    # read_conn must stay in a single open transaction (autocommit=False, never commit)
    # so the named server-side cursor remains valid for the full iteration.
    # All commits happen only on write_conn.

    try:
        # Step 1: Make sure the table and columns exist
        ensure_schema(write_conn, args.dry_run, log)

        # Step 2: ETL
        run_etl(conn, write_conn, args.dry_run, args.batch_size, log, stats)

    except KeyboardInterrupt:
        log.warning("Interrupted by user — rolling back any open transaction.")
        write_conn.rollback()
    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=True)
        write_conn.rollback()
    finally:
        conn.close()
        write_conn.close()

    print_report(stats, args.dry_run)


if __name__ == "__main__":
    main()
