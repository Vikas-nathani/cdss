#!/usr/bin/env python3
"""
populate_drug_ingredient_mapping.py  (simplified — RxCUI matching)

Populates drugdb.drug_ingredient_mapping by extracting ingredient data from
public.DrugMasterLinkage.combined_clean_jsonb and matching it against the
drugdb.drug and public.ingredients tables.

Data path:
  combined_clean_jsonb -> rxnorm[] -> rxcui + ingredients[] -> name / scdc.mass / scdc.unit

Matching strategy (replaces the old string-based formulation matching):
  1. At startup, load the drug table into a dict:
         rxcui → List[formulation_id]
     O(1) per lookup, ~10 MB of RAM.
  2. Load the ingredients table into a dict:
         name.lower() → ingredient_id
  3. For every rxnorm entry, look up rxcui → formulation_ids,
     then for each ingredient look up name → ingredient_id.
  4. ON CONFLICT DO NOTHING makes re-runs safe.

No cleaning logic needed — rxcui has 100% coverage on the drug table.

Run order (strict dependency):
  1. populate_drug_table.py            (populates drugdb.drug)
  2. update_drug_rxnorm_columns.py     (fills drug.rxcui — 100% coverage required)
  3. THIS SCRIPT                       (populates drugdb.drug_ingredient_mapping)

Usage:
  python3 populate_drug_ingredient_mapping.py --password <pwd>
  python3 populate_drug_ingredient_mapping.py --password <pwd> --dry-run --limit 100 --verbose
  python3 populate_drug_ingredient_mapping.py --password <pwd> --verify --log-file logs/ingredient_mapping.log
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
from psycopg2.extras import execute_values


# ──────────────────────────────────────────────────────────────
# DDL
# ──────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS drugdb.drug_ingredient_mapping (
    formulation_id UUID    NOT NULL REFERENCES drugdb.drug(formulation_id),
    ingredient_id  UUID    NOT NULL REFERENCES drugdb.ingredients(id),
    mass           NUMERIC,
    unit           VARCHAR(50),
    PRIMARY KEY (formulation_id, ingredient_id)
);
"""

CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_dim_formulation_id ON drugdb.drug_ingredient_mapping (formulation_id);",
    "CREATE INDEX IF NOT EXISTS idx_dim_ingredient_id  ON drugdb.drug_ingredient_mapping (ingredient_id);",
]


# ──────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────

@dataclass
class Stats:
    source_records_read:             int = 0
    source_records_skipped:          int = 0
    rxnorm_entries_found:            int = 0
    rxnorm_entries_with_ingredients: int = 0
    rxnorm_entries_no_rxcui:         int = 0
    rxnorm_entries_no_ingredients:   int = 0
    successful_rxcui_matches:        int = 0
    failed_rxcui_matches:            int = 0
    formulation_ids_matched:         int = 0
    ingredient_lookups_attempted:    int = 0
    ingredient_lookups_successful:   int = 0
    ingredient_lookups_failed:       int = 0
    rows_inserted:                   int = 0
    errors:                          int = 0
    error_details: list = field(default_factory=list)
    failed_rxcui_samples: list  = field(default_factory=list)
    failed_ing_samples:   list  = field(default_factory=list)


# ──────────────────────────────────────────────────────────────
# Prerequisites
# ──────────────────────────────────────────────────────────────

def verify_prerequisites(conn, log: logging.Logger):
    """Abort early if required tables are missing or empty."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM drugdb.drug WHERE rxcui IS NOT NULL")
        drug_count = cur.fetchone()[0]
        if drug_count == 0:
            raise Exception(
                "Drug table has no rxcui values — run update_drug_rxnorm_columns.py first."
            )

        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE  table_schema = 'drugdb' AND table_name = 'ingredients'
        """)
        if cur.fetchone()[0] == 0:
            raise Exception("drugdb.ingredients table does not exist!")

        cur.execute("SELECT COUNT(*) FROM drugdb.ingredients")
        ing_count = cur.fetchone()[0]
        if ing_count == 0:
            raise Exception("drugdb.ingredients table is empty!")

        log.info(
            "Prerequisites verified: %d drugs with rxcui, %d ingredients",
            drug_count, ing_count,
        )


# ──────────────────────────────────────────────────────────────
# Schema setup
# ──────────────────────────────────────────────────────────────

def ensure_schema(conn, dry_run: bool, log: logging.Logger):
    with conn.cursor() as cur:
        log.info("Ensuring drugdb.drug_ingredient_mapping table exists …")
        if not dry_run:
            cur.execute(CREATE_TABLE_SQL)
            for idx_sql in CREATE_INDEX_SQL:
                cur.execute(idx_sql)
            conn.commit()
        else:
            log.info("DRY-RUN: skipping DDL execution")


# ──────────────────────────────────────────────────────────────
# In-memory lookups
# ──────────────────────────────────────────────────────────────

def load_rxcui_lookup(conn, log: logging.Logger) -> dict:
    """
    Load drugdb.drug into a dict:
        rxcui (str) → list of formulation_id strings

    A single rxcui can map to multiple formulation_ids when the same RxNorm
    concept appears in different DrugMasterLinkage records, or when one record
    produced rows for multiple dosage-form representations sharing the same rxcui.
    """
    # Only load formulation_ids not yet in drug_ingredient_mapping.
    # On a fresh run this returns all rows; on resume it returns only uncovered ones.
    log.info("Loading rxcui → formulation_id lookup (uncovered formulations only) …")
    t0 = datetime.now()
    lookup: dict = {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.rxcui, d.formulation_id::text
            FROM   drugdb.drug d
            WHERE  d.rxcui IS NOT NULL
              AND  NOT EXISTS (
                  SELECT 1 FROM drugdb.drug_ingredient_mapping dim
                  WHERE  dim.formulation_id = d.formulation_id
              )
        """)
        for rxcui, fid in cur:
            lookup.setdefault(rxcui, []).append(fid)

    elapsed = (datetime.now() - t0).total_seconds()
    total_fids = sum(len(v) for v in lookup.values())
    multi      = sum(1 for v in lookup.values() if len(v) > 1)
    log.info(
        "Lookup loaded: %d unique rxcuis → %d uncovered formulation_ids in %.2fs "
        "(%d rxcuis map to >1 formulation_id)",
        len(lookup), total_fids, elapsed, multi,
    )
    if total_fids == 0:
        log.info("All formulations already have ingredient mappings — nothing to do.")
    return lookup


def load_ingredient_lookup(conn, log: logging.Logger) -> dict:
    """Load public.ingredients into name.lower() → id (str) mapping."""
    log.info("Loading name → ingredient_id lookup from public.ingredients …")
    t0 = datetime.now()
    lookup: dict = {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id::text, lower(trim(name)) AS name_lower
            FROM   drugdb.ingredients
            WHERE  name IS NOT NULL
        """)
        for ing_id, name_lower in cur:
            if name_lower not in lookup:
                lookup[name_lower] = ing_id

    elapsed = (datetime.now() - t0).total_seconds()
    log.info("Loaded %d ingredient entries into lookup in %.2fs", len(lookup), elapsed)
    return lookup


# ──────────────────────────────────────────────────────────────
# ETL core
# ──────────────────────────────────────────────────────────────

FETCH_SQL = """
    SELECT master_linkage_id::text, combined_clean_jsonb
    FROM   public."DrugMasterLinkage"
    WHERE  combined_clean_jsonb IS NOT NULL
      AND  combined_clean_jsonb ? 'rxnorm'
"""

INSERT_SQL = """
    INSERT INTO drugdb.drug_ingredient_mapping
        (formulation_id, ingredient_id, mass, unit)
    VALUES %s
    ON CONFLICT (formulation_id, ingredient_id) DO NOTHING
"""


def run_etl(
    read_conn,
    write_conn,
    rxcui_lookup:      dict,
    ingredient_lookup: dict,
    dry_run:    bool,
    batch_size: int,
    limit:      Optional[int],
    verbose:    bool,
    log:        logging.Logger,
    stats:      Stats,
):
    src_cur = read_conn.cursor(
        name="dim_stream",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    src_cur.itersize = 1000

    query = FETCH_SQL
    if limit:
        query += f" LIMIT {limit}"
    src_cur.execute(query)

    write_cur = write_conn.cursor()
    batch: list = []

    def flush_batch():
        if not batch or dry_run:
            batch.clear()
            return
        try:
            execute_values(
                write_cur, INSERT_SQL, batch,
                template="(%s::uuid, %s::uuid, %s, %s)",
                page_size=10000,
            )
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
            ingredients = entry.get("ingredients")

            if not rxcui:
                stats.rxnorm_entries_no_rxcui += 1
                continue

            if not isinstance(ingredients, list) or not ingredients:
                stats.rxnorm_entries_no_ingredients += 1
                continue

            stats.rxnorm_entries_with_ingredients += 1

            # O(1) lookup — no string cleaning needed
            formulation_ids = rxcui_lookup.get(rxcui)
            if not formulation_ids:
                stats.failed_rxcui_matches += 1
                if len(stats.failed_rxcui_samples) < 50:
                    stats.failed_rxcui_samples.append(rxcui)
                log.debug("No uncovered formulation_id for rxcui=%s", rxcui)
                continue

            stats.successful_rxcui_matches += 1
            stats.formulation_ids_matched  += len(formulation_ids)

            for ing in ingredients:
                if not isinstance(ing, dict):
                    continue

                ing_name = (ing.get("name") or "").strip()
                if not ing_name:
                    continue

                stats.ingredient_lookups_attempted += 1

                scdc = ing.get("scdc") or {}
                mass = scdc.get("mass")
                unit = (scdc.get("unit") or "").strip() or None

                if mass is not None:
                    try:
                        mass = float(mass)
                    except (ValueError, TypeError):
                        log.debug("Non-numeric mass %r for %r — storing NULL", mass, ing_name)
                        mass = None

                ingredient_id = ingredient_lookup.get(ing_name.lower())
                if not ingredient_id:
                    stats.ingredient_lookups_failed += 1
                    if len(stats.failed_ing_samples) < 50:
                        stats.failed_ing_samples.append(ing_name)
                    log.debug("Ingredient not found: %r", ing_name)
                    continue

                stats.ingredient_lookups_successful += 1

                for fid in formulation_ids:
                    if verbose and dry_run:
                        log.debug(
                            "DRY-RUN | fid=%s | rxcui=%-12s | ing=%r | mass=%s %s",
                            fid, rxcui, ing_name[:40], mass, unit or "",
                        )
                    if dry_run:
                        stats.rows_inserted += 1
                    else:
                        batch.append((fid, ingredient_id, mass, unit))
                        stats.rows_inserted += 1

            if not dry_run and len(batch) >= batch_size:
                flush_batch()

        if stats.source_records_read % 5000 == 0:
            rxcui_total = stats.successful_rxcui_matches + stats.failed_rxcui_matches
            rxcui_pct   = 100.0 * stats.successful_rxcui_matches / rxcui_total if rxcui_total else 0.0
            ing_pct     = (
                100.0 * stats.ingredient_lookups_successful / stats.ingredient_lookups_attempted
                if stats.ingredient_lookups_attempted else 0.0
            )
            log.info(
                "Progress: %d records | %d rxnorm entries | %d inserted | "
                "rxcui=%.1f%% ing=%.1f%% | errors=%d",
                stats.source_records_read,
                stats.rxnorm_entries_found,
                stats.rows_inserted,
                rxcui_pct,
                ing_pct,
                stats.errors,
            )
            if rxcui_total and rxcui_pct < 95.0:
                log.warning("ALERT: rxcui match rate %.1f%% is below 95%%!", rxcui_pct)
            if stats.ingredient_lookups_attempted and ing_pct < 80.0:
                log.warning("ALERT: ingredient match rate %.1f%% is below 80%%!", ing_pct)

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
        cur.execute("SELECT COUNT(*) FROM drugdb.drug_ingredient_mapping")
        total_rows = cur.fetchone()[0]

        cur.execute("""
            SELECT
                COUNT(DISTINCT d.formulation_id)   AS total_formulations,
                COUNT(DISTINCT dim.formulation_id) AS with_ingredients,
                COUNT(DISTINCT d.formulation_id)
                    - COUNT(DISTINCT dim.formulation_id) AS without_ingredients
            FROM drugdb.drug d
            LEFT JOIN drugdb.drug_ingredient_mapping dim
                   ON dim.formulation_id = d.formulation_id
        """)
        cov = cur.fetchone()

        print("\n" + "=" * 60)
        print("  Verification Summary")
        print("=" * 60)
        print(f"  Total rows in drug_ingredient_mapping  : {total_rows:>10,}")
        print(f"  Formulations with ingredients          : {cov[1]:>10,}")
        print(f"  Formulations without ingredients       : {cov[2]:>10,}")
        if cov[0]:
            print(f"  Coverage                               : {100*cov[1]/cov[0]:>9.2f}%")

        cur.execute("""
            SELECT ingredient_count, COUNT(*) AS formulations
            FROM (
                SELECT formulation_id, COUNT(*) AS ingredient_count
                FROM drugdb.drug_ingredient_mapping
                GROUP BY formulation_id
            ) t
            GROUP BY ingredient_count
            ORDER BY ingredient_count
            LIMIT 15
        """)
        print("\n  Ingredients per formulation distribution:")
        for row in cur.fetchall():
            print(f"    {row[0]:>3} ingredient(s): {row[1]:>8,} formulations")

        print("\n  Sample mappings (first 20 by drug name):")
        cur.execute("""
            SELECT
                d.generic_name,
                d.generic_formulation,
                d.rxcui,
                i.name  AS ingredient_name,
                dim.mass,
                dim.unit
            FROM drugdb.drug_ingredient_mapping dim
            JOIN drugdb.drug        d ON d.formulation_id = dim.formulation_id
            JOIN drugdb.ingredients i ON i.id             = dim.ingredient_id
            ORDER BY d.generic_name
            LIMIT 20
        """)
        for k, r in enumerate(cur.fetchall(), 1):
            print(f"  {k:2d}. {(r[0] or '')!r:.25s} | rxcui={r[2]} | "
                  f"ing={r[3]!r:.30s} | {r[4]} {r[5] or ''}")

        print("\n  Formulations without ingredients (first 10):")
        cur.execute("""
            SELECT d.formulation_id, d.generic_name, d.rxcui
            FROM drugdb.drug d
            LEFT JOIN drugdb.drug_ingredient_mapping dim
                   ON dim.formulation_id = d.formulation_id
            WHERE  dim.formulation_id IS NULL
            LIMIT 10
        """)
        no_ing = cur.fetchall()
        if no_ing:
            for r in no_ing:
                print(f"    rxcui={r[2]} | {(r[1] or '')!r:.50s}")
        else:
            print("    None — all formulations have ingredients.")
        print("=" * 60)


# ──────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────

REPORT_TEMPLATE = """
============================================================
  drug_ingredient_mapping Population Summary
  ({timestamp})
  {dry_run_banner}============================================================
  Source records processed                  : {source_records_read:>10,}
  Source records skipped (no rxnorm/jsonb)  : {source_records_skipped:>10,}
  RxNorm entries found                      : {rxnorm_entries_found:>10,}

  Entries skipped — no rxcui                : {no_rxcui:>10,}
  Entries skipped — no ingredients array    : {no_ingredients:>10,}
  Entries with ingredients (attempted)      : {with_ingredients:>10,}

  Successful rxcui matches                  : {successful_rxcui_matches:>10,}
  Failed rxcui matches                      : {failed_rxcui_matches:>10,}
  Total formulation_ids matched             : {formulation_ids_matched:>10,}

  Ingredient lookups attempted              : {ing_attempted:>10,}
  Ingredient lookups successful             : {ing_successful:>10,}
  Ingredient lookups failed                 : {ing_failed:>10,}

  Rows inserted (or would be inserted)      : {rows_inserted:>10,}
  Errors                                    : {errors:>10,}
============================================================
"""


def print_report(stats: Stats, dry_run: bool):
    banner = "*** DRY-RUN — NO DATA WRITTEN ***\n  " if dry_run else ""
    print(REPORT_TEMPLATE.format(
        timestamp                = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        dry_run_banner           = banner,
        source_records_read      = stats.source_records_read,
        source_records_skipped   = stats.source_records_skipped,
        rxnorm_entries_found     = stats.rxnorm_entries_found,
        no_rxcui                 = stats.rxnorm_entries_no_rxcui,
        no_ingredients           = stats.rxnorm_entries_no_ingredients,
        with_ingredients         = stats.rxnorm_entries_with_ingredients,
        successful_rxcui_matches = stats.successful_rxcui_matches,
        failed_rxcui_matches     = stats.failed_rxcui_matches,
        formulation_ids_matched  = stats.formulation_ids_matched,
        ing_attempted            = stats.ingredient_lookups_attempted,
        ing_successful           = stats.ingredient_lookups_successful,
        ing_failed               = stats.ingredient_lookups_failed,
        rows_inserted            = stats.rows_inserted,
        errors                   = stats.errors,
    ))

    rxcui_total = stats.successful_rxcui_matches + stats.failed_rxcui_matches
    if rxcui_total:
        print(f"  RxCUI match rate    : {100.0 * stats.successful_rxcui_matches / rxcui_total:.1f}%")
    if stats.ingredient_lookups_attempted:
        print(f"  Ingredient match rate: "
              f"{100.0 * stats.ingredient_lookups_successful / stats.ingredient_lookups_attempted:.1f}%")

    if stats.failed_rxcui_samples:
        print(f"\n  Failed rxcuis (first {len(stats.failed_rxcui_samples)}):")
        for r in stats.failed_rxcui_samples[:20]:
            print(f"    rxcui={r}")

    if stats.failed_ing_samples:
        unique_failed = list(dict.fromkeys(stats.failed_ing_samples))
        print(f"\n  Failed ingredient lookups (first {min(20, len(unique_failed))}):")
        for name in unique_failed[:20]:
            print(f"    {name!r}")

    if stats.error_details:
        print("  Error details (first 20):")
        for d in stats.error_details[:20]:
            print(f"    {d}")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        description="Populate drugdb.drug_ingredient_mapping using direct RxCUI matching"
    )
    p.add_argument("--host",       default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--dbname",     default="postgres")
    p.add_argument("--user",       default="postgres")
    p.add_argument("--password",   required=True)
    p.add_argument("--port",       type=int, default=5432)
    p.add_argument("--dry-run",    action="store_true",
                   help="Show what would be inserted without writing anything")
    p.add_argument("--batch-size", type=int, default=50000,
                   help="Commit every N rows (default: 50000)")
    p.add_argument("--limit",      type=int, default=None,
                   help="Process only the first N DrugMasterLinkage records (for testing)")
    p.add_argument("--log-file",   help="Write detailed log to this file")
    p.add_argument("--verbose",    action="store_true")
    p.add_argument("--verify",     action="store_true",
                   help="Run verification queries after population")
    return p


def setup_logging(verbose: bool, log_file: Optional[str]) -> logging.Logger:
    log = logging.getLogger("dim_populate")
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
        verify_prerequisites(write_conn, log)
        ensure_schema(write_conn, args.dry_run, log)
        rxcui_lookup      = load_rxcui_lookup(write_conn, log)
        ingredient_lookup = load_ingredient_lookup(write_conn, log)

        run_etl(
            read_conn, write_conn, rxcui_lookup, ingredient_lookup,
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
