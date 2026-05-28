#!/usr/bin/env python3
"""
populate_drug_synonym_formulation.py  (simplified — RxCUI matching)

Reads public.DrugMasterLinkage.combined_clean_jsonb and populates
drugdb.drug_synonym_formulation with one row per (formulation_id, synonyms) pair.

Matching strategy (replaces the old two-stage cleaning approach):
  1. At startup, load the entire drug table into a dict:
         rxcui → List[formulation_id]
     This is O(1) per lookup and ~10 MB of RAM.
  2. For every rxnorm entry in DrugMasterLinkage, look up the rxcui
     and insert synonyms for every matching formulation_id.
  3. ON CONFLICT DO NOTHING makes re-runs safe.

No cleaning logic is needed because rxcui is already stored on every
drug row (100% coverage after Step 1b).

Usage:
  python3 populate_drug_synonym_formulation.py --password <pwd>
  python3 populate_drug_synonym_formulation.py --password <pwd> --dry-run --limit 100 --verbose
  python3 populate_drug_synonym_formulation.py --password <pwd> --verify --log-file logs/synonym_population.log
"""

import argparse
import ast
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras


# ──────────────────────────────────────────────────────────────
# DDL
# ──────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS drugdb.drug_synonym_formulation (
    id             SERIAL  PRIMARY KEY,
    formulation_id UUID    REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    synonyms       TEXT[]
);
"""

UNIQUE_CONSTRAINT_NAME = "uq_drug_synonym_formulation_formulation_id"
INDEX_NAME             = "idx_drug_synonym_formulation_formulation_id"
GIN_INDEX_NAME         = "idx_dsf_synonyms_gin"


def ensure_schema(conn, dry_run: bool, log: logging.Logger):
    with conn.cursor() as cur:
        log.info("Ensuring drugdb.drug_synonym_formulation table exists …")
        if not dry_run:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()

        # Unique constraint on formulation_id
        cur.execute("""
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t     ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'public'
              AND t.relname = 'drug_synonym_formulation'
              AND c.conname = %s AND c.contype = 'u'
        """, (UNIQUE_CONSTRAINT_NAME,))
        if cur.fetchone() is None:
            log.info("Unique constraint missing — adding …")
            if not dry_run:
                cur.execute(
                    f"ALTER TABLE drugdb.drug_synonym_formulation "
                    f"ADD CONSTRAINT {UNIQUE_CONSTRAINT_NAME} UNIQUE (formulation_id);"
                )
                conn.commit()
        else:
            log.info("Unique constraint already present.")

        # B-tree index on formulation_id
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname='public' "
            "AND tablename='drug_synonym_formulation' AND indexname=%s",
            (INDEX_NAME,),
        )
        if cur.fetchone() is None:
            log.info("Index '%s' missing — creating …", INDEX_NAME)
            if not dry_run:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {INDEX_NAME} "
                    "ON drugdb.drug_synonym_formulation (formulation_id);"
                )
                conn.commit()
        else:
            log.info("Index '%s' already present.", INDEX_NAME)

        # GIN index on synonyms array
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname='public' "
            "AND tablename='drug_synonym_formulation' AND indexname=%s",
            (GIN_INDEX_NAME,),
        )
        if cur.fetchone() is None:
            log.info("GIN index '%s' missing — creating …", GIN_INDEX_NAME)
            if not dry_run:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {GIN_INDEX_NAME} "
                    "ON drugdb.drug_synonym_formulation USING GIN (synonyms);"
                )
                conn.commit()
        else:
            log.info("GIN index '%s' already present.", GIN_INDEX_NAME)


# ──────────────────────────────────────────────────────────────
# In-memory RxCUI → [formulation_id] lookup
# ──────────────────────────────────────────────────────────────

def load_rxcui_lookup(conn, log: logging.Logger) -> dict[str, list[str]]:
    """
    Load only UNCOVERED formulation_ids into the rxcui lookup dict.
    On a fresh run this returns all 88,983 rows.
    On resume it returns only the formulation_ids not yet in drug_synonym_formulation,
    so no ON CONFLICT operations are needed and no work is repeated.
    """
    log.info("Loading rxcui → uncovered formulation_id lookup …")
    t0 = datetime.now()
    lookup: dict[str, list[str]] = {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.rxcui, d.formulation_id::text
            FROM   drugdb.drug d
            WHERE  d.rxcui IS NOT NULL
              AND  NOT EXISTS (
                  SELECT 1 FROM drugdb.drug_synonym_formulation dsf
                  WHERE  dsf.formulation_id = d.formulation_id
              )
        """)
        for rxcui, fid in cur:
            lookup.setdefault(rxcui, []).append(fid)

    elapsed = (datetime.now() - t0).total_seconds()
    total_fids = sum(len(v) for v in lookup.values())
    multi = sum(1 for v in lookup.values() if len(v) > 1)
    log.info(
        "Lookup loaded: %d unique rxcuis → %d uncovered formulation_ids in %.2fs "
        "(%d rxcuis map to >1 formulation_id)",
        len(lookup), total_fids, elapsed, multi,
    )
    if total_fids == 0:
        log.info("All formulations already have synonyms — nothing to do.")
    return lookup


# ──────────────────────────────────────────────────────────────
# Synonym parsing
# ──────────────────────────────────────────────────────────────

def parse_synonyms(raw) -> Optional[list[str]]:
    """
    Normalise the synonyms field into a list[str] or None.
    Handles both native JSON arrays and Python-repr strings.
    """
    if isinstance(raw, list):
        result = [str(s) for s in raw if s]
        return result or None

    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        if stripped.startswith("["):
            try:
                parsed = ast.literal_eval(stripped)
                if isinstance(parsed, list):
                    result = [str(s) for s in parsed if s]
                    return result or None
            except (ValueError, SyntaxError):
                pass
        return [stripped]

    return None


# ──────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────

@dataclass
class Stats:
    source_records_read:          int = 0
    source_records_skipped:       int = 0
    rxnorm_entries_found:         int = 0
    rxnorm_entries_with_synonyms: int = 0
    rxnorm_entries_no_rxcui:      int = 0
    rxnorm_entries_no_synonyms:   int = 0
    successful_matches:           int = 0
    failed_matches:               int = 0
    rows_inserted:                int = 0
    errors:                       int = 0
    error_details: list = field(default_factory=list)
    failed_samples: list = field(default_factory=list)


# ──────────────────────────────────────────────────────────────
# ETL core
# ──────────────────────────────────────────────────────────────

FETCH_SQL = """
    SELECT m.master_linkage_id::text, m.combined_clean_jsonb
    FROM   public."DrugMasterLinkage" m
    WHERE  m.combined_clean_jsonb IS NOT NULL
      AND  EXISTS (
          SELECT 1 FROM drugdb.drug d
          WHERE  d.master_linkage_id = m.master_linkage_id
            AND  d.rxcui IS NOT NULL
            AND  NOT EXISTS (
                SELECT 1 FROM drugdb.drug_synonym_formulation dsf
                WHERE  dsf.formulation_id = d.formulation_id
            )
      )
"""

INSERT_SQL = """
    INSERT INTO drugdb.drug_synonym_formulation (formulation_id, synonyms)
    VALUES (%s::uuid, %s)
    ON CONFLICT ON CONSTRAINT uq_drug_synonym_formulation_formulation_id
    DO NOTHING
"""


def run_etl(
    read_conn,
    write_conn,
    rxcui_lookup: dict[str, list[str]],
    dry_run: bool,
    batch_size: int,
    limit: Optional[int],
    verbose: bool,
    log: logging.Logger,
    stats: Stats,
):
    src_cur = read_conn.cursor(
        name="dsf_stream",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    src_cur.itersize = 500

    query = FETCH_SQL
    if limit:
        query += f" LIMIT {limit}"
    src_cur.execute(query)

    write_cur = write_conn.cursor()
    batch: list[tuple] = []

    def flush_batch():
        if not batch or dry_run:
            batch.clear()
            return
        try:
            write_cur.executemany(INSERT_SQL, batch)
            write_conn.commit()
            log.info("  → Committed %d rows (total inserted: %d)",
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
        stats.source_records_read += 1
        jsonb = raw_row["combined_clean_jsonb"]

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
            rxcui       = (entry.get("rxcui") or "").strip() or None
            raw_synonyms = entry.get("synonyms")

            if not rxcui:
                stats.rxnorm_entries_no_rxcui += 1
                continue

            synonyms = parse_synonyms(raw_synonyms)
            if synonyms is None:
                stats.rxnorm_entries_no_synonyms += 1
                continue

            stats.rxnorm_entries_with_synonyms += 1

            formulation_ids = rxcui_lookup.get(rxcui)
            if not formulation_ids:
                stats.failed_matches += 1
                if len(stats.failed_samples) < 50:
                    stats.failed_samples.append({
                        "rxcui": rxcui,
                        "synonyms_count": len(synonyms),
                    })
                log.warning("No formulation_id for rxcui=%s", rxcui)
                continue

            stats.successful_matches += 1

            for fid in formulation_ids:
                if verbose and dry_run:
                    log.debug(
                        "DRY-RUN | fid=%s | rxcui=%-12s | synonyms=%d | %r…",
                        fid, rxcui, len(synonyms), synonyms[0][:40] if synonyms else "",
                    )
                if dry_run:
                    stats.rows_inserted += 1
                else:
                    batch.append((fid, synonyms))
                    stats.rows_inserted += 1

            if not dry_run and len(batch) >= batch_size:
                flush_batch()

        if stats.source_records_read % 5000 == 0:
            log.info(
                "Progress: %d records | %d rxnorm entries | %d inserted | "
                "no_rxcui:%d no_synonyms:%d failed:%d | %d errors",
                stats.source_records_read, stats.rxnorm_entries_found,
                stats.rows_inserted,
                stats.rxnorm_entries_no_rxcui,
                stats.rxnorm_entries_no_synonyms,
                stats.failed_matches,
                stats.errors,
            )

    if batch:
        flush_batch()

    src_cur.close()
    write_cur.close()


# ──────────────────────────────────────────────────────────────
# Verification
# ──────────────────────────────────────────────────────────────

def run_verification(conn, log: logging.Logger):
    log.info("Running post-population verification …")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM drugdb.drug_synonym_formulation")
        total_rows = cur.fetchone()[0]

        cur.execute("""
            SELECT
                COUNT(DISTINCT d.formulation_id)   AS total_formulations,
                COUNT(DISTINCT dsf.formulation_id) AS with_synonyms,
                COUNT(DISTINCT d.formulation_id) - COUNT(DISTINCT dsf.formulation_id)
                                                   AS without_synonyms
            FROM drugdb.drug d
            LEFT JOIN drugdb.drug_synonym_formulation dsf
                   ON dsf.formulation_id = d.formulation_id
        """)
        cov = cur.fetchone()

        print("\n" + "=" * 60)
        print("  Verification Summary")
        print("=" * 60)
        print(f"  Total rows in drug_synonym_formulation : {total_rows:>10,}")
        print(f"  Formulations with synonyms             : {cov[1]:>10,}")
        print(f"  Formulations without synonyms          : {cov[2]:>10,}")
        if cov[0]:
            print(f"  Coverage                               : {100*cov[1]/cov[0]:>9.2f}%")
        print("=" * 60)

        print("\n  Sample rows (top 10 by synonym count):")
        cur.execute("""
            SELECT
                d.generic_name,
                d.generic_formulation,
                d.rxcui,
                array_length(dsf.synonyms, 1) AS syn_count,
                dsf.synonyms[1:3]             AS sample_synonyms
            FROM drugdb.drug_synonym_formulation dsf
            JOIN drugdb.drug d ON d.formulation_id = dsf.formulation_id
            ORDER BY array_length(dsf.synonyms, 1) DESC NULLS LAST
            LIMIT 10
        """)
        for i, r in enumerate(cur.fetchall(), 1):
            print(f"  {i:2d}. {r[0]!r:.30s} | rxcui={r[2]} | "
                  f"synonyms={r[3]} | sample={r[4]}")

        print("\n  Formulations without synonyms (first 10):")
        cur.execute("""
            SELECT d.formulation_id, d.generic_name, d.generic_formulation, d.rxcui
            FROM drugdb.drug d
            LEFT JOIN drugdb.drug_synonym_formulation dsf
                   ON dsf.formulation_id = d.formulation_id
            WHERE dsf.id IS NULL
            LIMIT 10
        """)
        no_syn = cur.fetchall()
        if no_syn:
            for r in no_syn:
                print(f"    rxcui={r[3]} | {r[1]!r:.30s} | {r[2]!r:.50s}")
        else:
            print("    None — all formulations have synonyms.")


# ──────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────

REPORT_TEMPLATE = """
============================================================
  drug_synonym_formulation Population Summary
  ({timestamp})
  {dry_run_banner}============================================================
  Source records processed                  : {source_records_read:>10,}
  Source records skipped (no rxnorm/jsonb)  : {source_records_skipped:>10,}
  RxNorm entries found                      : {rxnorm_entries_found:>10,}

  Entries skipped — no rxcui                : {no_rxcui:>10,}
  Entries skipped — no synonyms             : {no_synonyms:>10,}
  Entries with synonyms (attempted)         : {with_synonyms:>10,}

  Successful rxcui matches                  : {successful_matches:>10,}
  Failed matches (rxcui not in drug table)  : {failed_matches:>10,}
  Rows inserted (or would be inserted)      : {rows_inserted:>10,}
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
        no_rxcui              = stats.rxnorm_entries_no_rxcui,
        no_synonyms           = stats.rxnorm_entries_no_synonyms,
        with_synonyms         = stats.rxnorm_entries_with_synonyms,
        successful_matches    = stats.successful_matches,
        failed_matches        = stats.failed_matches,
        rows_inserted         = stats.rows_inserted,
        errors                = stats.errors,
    ))
    if stats.failed_samples:
        print(f"  Failed rxcuis (first {len(stats.failed_samples)}):")
        for s in stats.failed_samples[:20]:
            print(f"    rxcui={s['rxcui']} | synonyms_count={s['synonyms_count']}")
    if stats.error_details:
        print("  Error details (first 20):")
        for d in stats.error_details[:20]:
            print(f"    {d}")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        description="Populate drugdb.drug_synonym_formulation using direct RxCUI matching"
    )
    p.add_argument("--host",       default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--dbname",     default="postgres")
    p.add_argument("--user",       default="postgres")
    p.add_argument("--password",   required=True)
    p.add_argument("--port",       type=int, default=5432)
    p.add_argument("--dry-run",    action="store_true",
                   help="Show what would be inserted without writing anything")
    p.add_argument("--batch-size", type=int, default=5000,
                   help="Commit every N rows (default: 5000)")
    p.add_argument("--limit",      type=int, default=None,
                   help="Process only the first N DrugMasterLinkage records (for testing)")
    p.add_argument("--log-file",   help="Write detailed log to this file")
    p.add_argument("--verbose",    action="store_true")
    p.add_argument("--verify",     action="store_true",
                   help="Run verification queries after population")
    return p


def setup_logging(verbose: bool, log_file: Optional[str]) -> logging.Logger:
    log = logging.getLogger("drug_synonym_populate")
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
    write_conn = connect(args, log)
    read_conn  = connect(args, log)
    read_conn.autocommit = False

    try:
        ensure_schema(write_conn, args.dry_run, log)
        rxcui_lookup = load_rxcui_lookup(write_conn, log)

        run_etl(
            read_conn, write_conn, rxcui_lookup,
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
        log.warning("Interrupted — rolling back.")
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
