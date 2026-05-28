#!/usr/bin/env python3
"""
update_drug_new_columns.py

Populates 9 new enrichment columns on drugdb.drug by streaming a JOIN between
drugdb.drug and public."DrugMasterLinkage" via a server-side cursor.

Memory strategy: NO bulk load into Python dicts. The DB does the join and
streams (formulation_id, combined_clean_jsonb) in batches of --batch-size rows.
Peak Python memory = one batch worth of rows, not all 50k JSONB blobs.

New columns populated:
  product_type         TEXT
  routes               TEXT[]
  mechanism_of_action  TEXT
  record_version       TEXT
  last_ingested_at     TIMESTAMPTZ
  has_openfda          BOOLEAN
  has_dailymed         BOOLEAN
  has_rxnorm           BOOLEAN
  has_drugbank         BOOLEAN

NEVER touches: formulation_id, master_linkage_id, generic_name,
               generic_formulation, rxnorm_generic_formulation, rxcui,
               dosage_forms, generic_formulation_original

Usage:
  python3 update_drug_new_columns.py --password <pwd> --dry-run --verbose
  python3 update_drug_new_columns.py --password <pwd> --log-file logs/drug_enrichment.log
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "postgres"
MOA_MAX_CHARS = 5000

# Server-side cursor streams this many rows per fetch from the DB
FETCH_SIZE = 500


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Populate new enrichment columns on drugdb.drug (memory-efficient)"
    )
    p.add_argument("--password", required=True, help="PostgreSQL password")
    p.add_argument("--dry-run", action="store_true",
                   help="Print 10 sample rows without writing to DB")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Rows per executemany batch (default 1000)")
    p.add_argument("--verbose", action="store_true",
                   help="Extra logging")
    p.add_argument("--log-file", type=str, default=None,
                   help="Path to log file (e.g. logs/drug_enrichment.log)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_file: Optional[str], verbose: bool) -> logging.Logger:
    logger = logging.getLogger("drug_enrichment")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.info(f"Logging to {log_file}")

    return logger


# ---------------------------------------------------------------------------
# JSON extraction helpers
# ---------------------------------------------------------------------------

def extract_product_type(jsonb: dict) -> Optional[str]:
    try:
        val = jsonb["dailymed"]["identification"]["drug_label"]["label_type"]
        if val:
            val = val.strip()
            if val.upper().endswith(" LABEL"):
                val = val[:-6].strip()
        return val or None
    except (KeyError, TypeError):
        return None


def extract_routes(jsonb: dict) -> Optional[list]:
    try:
        products = jsonb["dailymed"]["drug_info"]["products"]
        if not isinstance(products, list):
            return None
        seen: set = set()
        for prod in products:
            route = prod.get("route_of_administration")
            if route and isinstance(route, str):
                seen.add(route.strip())
        return sorted(seen) if seen else None
    except (KeyError, TypeError):
        return None


def extract_moa(jsonb: dict) -> Optional[str]:
    try:
        text = jsonb["openfda"]["clinical"]["mechanism_of_action"]["text"]
        if not text:
            return None
        # Some records store this as a JSON array instead of a string
        if isinstance(text, list):
            text = " ".join(str(t) for t in text if t)
        else:
            text = str(text)
        text = text.strip()
        if not text:
            return None
        if len(text) > MOA_MAX_CHARS:
            text = text[:MOA_MAX_CHARS] + "..."
        return text
    except (KeyError, TypeError):
        return None


def extract_record_version(jsonb: dict) -> str:
    try:
        val = jsonb["dailymed"]["identification"]["drug_label"]["version"]
        return str(val).strip() if val else "1.0"
    except (KeyError, TypeError):
        return "1.0"


def extract_all(jsonb) -> dict:
    if not isinstance(jsonb, dict):
        try:
            jsonb = json.loads(jsonb)
        except Exception:
            jsonb = {}

    rxnorm = jsonb.get("rxnorm")
    drugbank = jsonb.get("drugbank")

    return {
        "product_type":        extract_product_type(jsonb),
        "routes":              extract_routes(jsonb),
        "mechanism_of_action": extract_moa(jsonb),
        "record_version":      extract_record_version(jsonb),
        "has_openfda":         jsonb.get("openfda") is not None,
        "has_dailymed":        jsonb.get("dailymed") is not None,
        "has_rxnorm":          isinstance(rxnorm, list) and len(rxnorm) > 0,
        "has_drugbank":        isinstance(drugbank, list) and len(drugbank) > 0,
    }


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# Single JOIN query — DB does the work, Python never holds the full JSONB dict
STREAM_SQL = """
SELECT
    d.formulation_id,
    dml.combined_clean_jsonb
FROM drugdb.drug d
JOIN public."DrugMasterLinkage" dml
  ON dml.master_linkage_id = d.master_linkage_id
WHERE dml.combined_clean_jsonb IS NOT NULL
"""

UPDATE_SQL = """
UPDATE drugdb.drug SET
    product_type        = %(product_type)s,
    routes              = %(routes)s,
    mechanism_of_action = %(mechanism_of_action)s,
    record_version      = %(record_version)s,
    last_ingested_at    = %(last_ingested_at)s,
    has_openfda         = %(has_openfda)s,
    has_dailymed        = %(has_dailymed)s,
    has_rxnorm          = %(has_rxnorm)s,
    has_drugbank        = %(has_drugbank)s
WHERE formulation_id = %(formulation_id)s
"""

COUNT_SQL = """
SELECT COUNT(*)
FROM drugdb.drug d
JOIN public."DrugMasterLinkage" dml
  ON dml.master_linkage_id = d.master_linkage_id
WHERE dml.combined_clean_jsonb IS NOT NULL
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    log = setup_logging(args.log_file, args.verbose)

    conn_kwargs = dict(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=args.password,
        connect_timeout=15,
    )

    try:
        # read_conn: never commits — keeps named cursor alive across the stream
        read_conn = psycopg2.connect(**conn_kwargs)
        read_conn.set_session(readonly=True)
        write_conn = psycopg2.connect(**conn_kwargs)
    except Exception as e:
        log.error(f"Could not connect to database: {e}")
        sys.exit(1)

    ingested_at = datetime.now(timezone.utc)

    try:
        # Get total for progress reporting
        with read_conn.cursor() as cur:
            cur.execute(COUNT_SQL)
            total_rows = cur.fetchone()[0]
        log.info(f"Rows to process (drug JOIN DrugMasterLinkage): {total_rows:,}")

        # Counters
        processed = 0
        updated   = 0
        errors    = 0
        cnt_openfda      = 0
        cnt_dailymed     = 0
        cnt_rxnorm       = 0
        cnt_drugbank     = 0
        cnt_routes       = 0
        cnt_moa          = 0
        cnt_product_type = 0

        # ----------------------------------------------------------------
        # DRY RUN — print 10 samples, no writes
        # ----------------------------------------------------------------
        if args.dry_run:
            log.info("DRY RUN mode — no writes will be made")
            with read_conn.cursor(name="dryrun_cursor") as cur:
                cur.itersize = FETCH_SIZE
                cur.execute(STREAM_SQL)
                rows = cur.fetchmany(10)

            log.info(f"--- DRY RUN: {len(rows)} sample rows ---")
            for formulation_id, jsonb in rows:
                vals = extract_all(jsonb)
                moa_preview = ""
                if vals["mechanism_of_action"]:
                    moa_preview = vals["mechanism_of_action"][:80] + ("..." if len(vals["mechanism_of_action"]) > 80 else "")
                log.info(
                    f"\n  formulation_id   : {formulation_id}\n"
                    f"  product_type     : {vals['product_type']}\n"
                    f"  routes           : {vals['routes']}\n"
                    f"  moa (preview)    : {moa_preview}\n"
                    f"  record_version   : {vals['record_version']}\n"
                    f"  has_openfda      : {vals['has_openfda']}\n"
                    f"  has_dailymed     : {vals['has_dailymed']}\n"
                    f"  has_rxnorm       : {vals['has_rxnorm']}\n"
                    f"  has_drugbank     : {vals['has_drugbank']}"
                )
            log.info("--- END DRY RUN --- Re-run without --dry-run to apply updates.")
            return

        # ----------------------------------------------------------------
        # LIVE RUN — stream JOIN, batch update
        # ----------------------------------------------------------------
        batch: list = []

        def flush_batch(batch: list) -> int:
            if not batch:
                return 0
            try:
                with write_conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, UPDATE_SQL, batch, page_size=len(batch))
                write_conn.commit()
                return len(batch)
            except Exception as e:
                write_conn.rollback()
                log.error(f"Batch flush error ({len(batch)} rows): {e}")
                return 0

        t_start = time.time()

        with read_conn.cursor(name="stream_cursor") as cur:
            cur.itersize = FETCH_SIZE
            cur.execute(STREAM_SQL)

            for formulation_id, jsonb in cur:
                processed += 1
                vals = extract_all(jsonb)

                batch.append({
                    "formulation_id":      formulation_id,
                    "product_type":        vals["product_type"],
                    "routes":              vals["routes"],
                    "mechanism_of_action": vals["mechanism_of_action"],
                    "record_version":      vals["record_version"],
                    "last_ingested_at":    ingested_at,
                    "has_openfda":         vals["has_openfda"],
                    "has_dailymed":        vals["has_dailymed"],
                    "has_rxnorm":          vals["has_rxnorm"],
                    "has_drugbank":        vals["has_drugbank"],
                })

                if vals["has_openfda"]:    cnt_openfda += 1
                if vals["has_dailymed"]:   cnt_dailymed += 1
                if vals["has_rxnorm"]:     cnt_rxnorm += 1
                if vals["has_drugbank"]:   cnt_drugbank += 1
                if vals["routes"]:         cnt_routes += 1
                if vals["mechanism_of_action"]: cnt_moa += 1
                if vals["product_type"]:   cnt_product_type += 1

                if len(batch) >= args.batch_size:
                    ok = flush_batch(batch)
                    updated += ok
                    if ok < len(batch):
                        errors += len(batch) - ok
                    batch = []

                if processed % 5000 == 0:
                    elapsed = time.time() - t_start
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta  = (total_rows - processed) / rate if rate > 0 else 0
                    log.info(
                        f"Progress: {processed:>7,} / {total_rows:,} "
                        f"({100*processed/total_rows:.1f}%)  "
                        f"{rate:.0f} rows/s  ETA {eta:.0f}s"
                    )

        # Flush remainder
        if batch:
            ok = flush_batch(batch)
            updated += ok
            if ok < len(batch):
                errors += len(batch) - ok

        elapsed_total = time.time() - t_start
        skipped = total_rows - processed

        log.info("=" * 60)
        log.info("FINAL SUMMARY")
        log.info("=" * 60)
        log.info(f"Total drug rows processed     : {processed:,}")
        log.info(f"Successfully updated          : {updated:,}")
        log.info(f"Skipped (no linkage match)    : {skipped:,}")
        log.info(f"Errors                        : {errors:,}")
        log.info(f"has_openfda True              : {cnt_openfda:,}")
        log.info(f"has_dailymed True             : {cnt_dailymed:,}")
        log.info(f"has_rxnorm True               : {cnt_rxnorm:,}")
        log.info(f"has_drugbank True             : {cnt_drugbank:,}")
        log.info(f"Routes populated              : {cnt_routes:,}")
        log.info(f"Mechanism of action populated : {cnt_moa:,}")
        log.info(f"Product type populated        : {cnt_product_type:,}")
        log.info(f"Elapsed time                  : {elapsed_total:.1f}s")
        log.info("=" * 60)

        if errors > 0:
            sys.exit(1)

    except Exception as e:
        log.error(f"FATAL: {e}")
        try:
            write_conn.rollback()
        except Exception:
            pass
        sys.exit(1)
    finally:
        try:
            read_conn.close()
            write_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
