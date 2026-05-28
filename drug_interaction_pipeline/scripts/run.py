#!/usr/bin/env python3
"""
populate_drug_interaction.py

Populates drugdb.drug_interaction by resolving ingredient-level interaction pairs
from drugdb.ingredient_interactions to formulation-level pairs via
drugdb.drug_ingredient_mapping.

Join path:
  drug_ingredient_mapping (subject side)
  → ingredients (subject ingredient)
  → ingredient_interactions (interaction pair)
  → ingredients (partner ingredient)
  → drug_ingredient_mapping (partner side)
  → drug (partner formulation)

One row per unique (subject_formulation_id, partner_formulation_id) pair.
interaction_id = subject_formulation_id || '_' || partner_formulation_id

Memory strategy:
  - drug_ingredient_mapping (92,570 rows) preloaded into a dict keyed by ingredient_id.
  - ingredient_interactions (2,910,556 rows) streamed via server-side cursor in batches
    of 10,000 — never fully loaded into memory.

Run after:
  1. schemas/ingredient_schema.sql                  (drugdb.ingredient_interactions)
  2. scripts/populate_drug_ingredient_mapping.py    (drugdb.drug_ingredient_mapping)
  3. schemas/drug_interaction_schema.sql            (DDL for this table)

Usage:
  python3 scripts/populate_drug_interaction.py --password <pwd>
  python3 scripts/populate_drug_interaction.py --password <pwd> --dry-run
  python3 scripts/populate_drug_interaction.py --password <pwd> \\
      --batch-size 5000 --log-file logs/drug_interaction_population.log
"""

import argparse
import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras


TOTAL_INTERACTION_PAIRS = 2_910_556


# ──────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────

@dataclass
class Stats:
    pairs_processed:              int = 0
    rows_inserted:                int = 0
    rows_skipped_no_formulation:  int = 0
    errors:                       int = 0
    error_details: list = field(default_factory=list)


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Populate drugdb.drug_interaction from ingredient-level interactions"
    )
    p.add_argument("--host",       default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--port",       type=int, default=5432)
    p.add_argument("--dbname",     default="postgres")
    p.add_argument("--user",       default="postgres")
    p.add_argument("--password",   required=True, help="PostgreSQL password")
    p.add_argument("--dry-run",    action="store_true",
                   help="Estimate rows without writing any data")
    p.add_argument("--batch-size", type=int, default=5000,
                   help="Rows per commit (default: 5000)")
    p.add_argument("--limit",      type=int, default=None,
                   help="Stop after processing N ingredient_interaction pairs (for testing/dry-run)")
    p.add_argument("--log-file",   default="logs/drug_interaction_population.log",
                   help="Path to log file (default: logs/drug_interaction_population.log)")
    return p.parse_args()


# ──────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────

def setup_logging(log_file: str) -> logging.Logger:
    log = logging.getLogger("dxi_populate")
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    try:
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        log.addHandler(fh)
    except OSError as exc:
        log.warning("Could not open log file %s: %s — logging to console only", log_file, exc)

    return log


# ──────────────────────────────────────────────────────────────
# Connection
# ──────────────────────────────────────────────────────────────

def get_connection(args: argparse.Namespace) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        connect_timeout=30,
    )


# ──────────────────────────────────────────────────────────────
# Preload maps
# ──────────────────────────────────────────────────────────────

def build_ingredient_to_formulations_map(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
) -> Dict[str, List[str]]:
    """
    Load drugdb.drug_ingredient_mapping into memory.
    Returns: ingredient_id (str UUID) → [formulation_id, ...]

    92,570 rows at ~200 bytes each ≈ ~18 MB — fits comfortably in memory.
    """
    log.info("Loading ingredient → formulations map from drug_ingredient_mapping …")
    t0 = time.monotonic()
    ing_to_fids: Dict[str, List[str]] = defaultdict(list)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT ingredient_id::text, formulation_id::text "
            "FROM drugdb.drug_ingredient_mapping"
        )
        for ing_id, fid in cur:
            ing_to_fids[ing_id].append(fid)

    elapsed = time.monotonic() - t0
    total_mappings = sum(len(v) for v in ing_to_fids.values())
    log.info(
        "Loaded %d ingredients → %d total formulation mappings in %.2fs",
        len(ing_to_fids), total_mappings, elapsed,
    )
    if not ing_to_fids:
        raise RuntimeError(
            "drug_ingredient_mapping is empty — run populate_drug_ingredient_mapping.py first."
        )
    return ing_to_fids


def build_evidence_level_map(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
) -> Dict[tuple, str]:
    """
    Optionally preload per-interaction evidence_level.

    Currently returns an empty dict; all rows use the column DEFAULT ('established').
    Extend here to pull per-pair evidence from DrugSourceMaster.standardized_records
    if DrugBank starts shipping evidence codes alongside interactions.
    """
    log.info("Evidence level map: using column DEFAULT 'established' for all rows")
    return {}


# ──────────────────────────────────────────────────────────────
# Batch insert
# ──────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO drugdb.drug_interaction
        (interaction_id, subject_formulation_id, partner_formulation_id,
         evidence_level, source_excerpt)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (interaction_id) DO NOTHING
"""


def process_batch(
    conn: psycopg2.extensions.connection,
    batch: list,
    dry_run: bool,
    stats: Stats,
    log: logging.Logger,
) -> None:
    if not batch:
        return
    if dry_run:
        stats.rows_inserted += len(batch)
        batch.clear()
        return
    try:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, batch)
        conn.commit()
        stats.rows_inserted += len(batch)
    except Exception as exc:
        conn.rollback()
        msg = f"Batch insert failed ({len(batch)} rows): {exc}"
        log.error(msg)
        stats.errors += 1
        stats.error_details.append(msg)
    finally:
        batch.clear()


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    log  = setup_logging(args.log_file)

    if args.dry_run:
        log.info("*** DRY-RUN MODE — no data will be written ***")
    if args.limit:
        log.info("*** LIMIT=%d — processing first %d interaction pairs only ***",
                 args.limit, args.limit)

    log.info(
        "Connecting to %s:%d/%s as %s", args.host, args.port, args.dbname, args.user
    )

    try:
        read_conn  = get_connection(args)
        write_conn = get_connection(args)
    except Exception as exc:
        log.error("Connection failed: %s", exc)
        sys.exit(1)

    read_conn.autocommit = False

    stats  = Stats()
    t_start = time.monotonic()

    try:
        ing_to_fids  = build_ingredient_to_formulations_map(write_conn, log)
        evidence_map = build_evidence_level_map(write_conn, log)

        log.info("Streaming drugdb.ingredient_interactions via server-side cursor …")
        log.info("Total pairs to process: ~%s", f"{TOTAL_INTERACTION_PAIRS:,}")

        # In dry-run + limit mode, use a targeted query that only returns pairs
        # where BOTH sides are mapped to formulations — guarantees sample hits.
        # Full-run (no limit) always uses the plain streaming cursor.
        if args.dry_run and args.limit:
            sample_sql = (
                "SELECT ii.id::text, ii.reacting_id::text, ii.description "
                "FROM drugdb.ingredient_interactions ii "
                "WHERE EXISTS ("
                "  SELECT 1 FROM drugdb.drug_ingredient_mapping "
                "  WHERE ingredient_id = ii.id) "
                "AND EXISTS ("
                "  SELECT 1 FROM drugdb.drug_ingredient_mapping "
                "  WHERE ingredient_id = ii.reacting_id) "
                f"LIMIT {args.limit}"
            )
            src_cur = read_conn.cursor()
            src_cur.execute(sample_sql)
        else:
            src_cur = read_conn.cursor(name="dxi_stream")
            src_cur.itersize = 10_000
            src_cur.execute(
                "SELECT id::text, reacting_id::text, description "
                "FROM drugdb.ingredient_interactions"
            )

        batch: list = []
        dry_run_rows_shown = 0

        for subject_ing_id, partner_ing_id, description in src_cur:
            stats.pairs_processed += 1

            subject_fids = ing_to_fids.get(subject_ing_id)
            partner_fids = ing_to_fids.get(partner_ing_id)

            if not subject_fids or not partner_fids:
                stats.rows_skipped_no_formulation += 1
                if args.limit and stats.pairs_processed >= args.limit:
                    break
                continue

            for s_fid in subject_fids:
                for p_fid in partner_fids:
                    if s_fid == p_fid:
                        continue
                    interaction_id = f"{s_fid}_{p_fid}"
                    evidence_level = evidence_map.get(
                        (subject_ing_id, partner_ing_id), "established"
                    )
                    row = (interaction_id, s_fid, p_fid, evidence_level, description)
                    batch.append(row)

                    if args.dry_run and args.limit and dry_run_rows_shown < args.limit:
                        excerpt = (description or "")[:100]
                        log.info(
                            "DRY-RUN ROW %d:\n"
                            "  subject_formulation_id : %s\n"
                            "  partner_formulation_id : %s\n"
                            "  interaction_id         : %s\n"
                            "  evidence_level         : %s\n"
                            "  source_excerpt (100ch) : %s",
                            dry_run_rows_shown + 1,
                            s_fid, p_fid, interaction_id, evidence_level,
                            excerpt if excerpt else "(NULL)",
                        )
                        dry_run_rows_shown += 1

            if len(batch) >= args.batch_size:
                process_batch(write_conn, batch, args.dry_run, stats, log)

            if args.limit and stats.pairs_processed >= args.limit:
                break

            if stats.pairs_processed % 50_000 == 0:
                elapsed = time.monotonic() - t_start
                rate = stats.pairs_processed / elapsed if elapsed > 0 else 0
                remaining_pairs = TOTAL_INTERACTION_PAIRS - stats.pairs_processed
                etr_s = remaining_pairs / rate if rate > 0 else 0
                etr_str = (
                    f"{int(etr_s // 3600)}h {int((etr_s % 3600) // 60)}m"
                    if etr_s >= 60 else f"{etr_s:.0f}s"
                )
                log.info(
                    "Processed %s / %s interaction pairs | Inserted: %s | "
                    "Skipped (no formulation): %s | Rate: %.0f pairs/s | ETR: %s",
                    f"{stats.pairs_processed:,}",
                    f"{TOTAL_INTERACTION_PAIRS:,}",
                    f"{stats.rows_inserted:,}",
                    f"{stats.rows_skipped_no_formulation:,}",
                    rate,
                    etr_str,
                )

        if batch:
            process_batch(write_conn, batch, args.dry_run, stats, log)

        src_cur.close()

    except KeyboardInterrupt:
        log.warning("Interrupted — rolling back.")
        write_conn.rollback()
    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=True)
        write_conn.rollback()
    finally:
        read_conn.close()
        write_conn.close()

    elapsed_total = time.monotonic() - t_start

    print("\n" + "=" * 60)
    print("  drugdb.drug_interaction Population Summary")
    if args.dry_run:
        print("  *** DRY-RUN — NO DATA WRITTEN ***")
    if args.limit:
        print(f"  *** SAMPLED FIRST {args.limit} PAIRS ONLY ***")
    print(f"  Run at   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"  Pairs sampled / processed              : {stats.pairs_processed:>12,}")
    print(f"  Rows inserted (or would be inserted)   : {stats.rows_inserted:>12,}")
    print(f"  Skipped (no formulation for ingredient): {stats.rows_skipped_no_formulation:>12,}")
    print(f"  Errors                                 : {stats.errors:>12,}")
    print(f"  Time taken                             : {elapsed_total:>11.1f}s")

    if args.dry_run and args.limit and stats.pairs_processed > 0:
        pairs_with_rows = stats.pairs_processed - stats.rows_skipped_no_formulation
        hit_rate = pairs_with_rows / stats.pairs_processed
        estimated_total = int(TOTAL_INTERACTION_PAIRS * hit_rate)
        avg_rows_per_hit = (
            stats.rows_inserted / pairs_with_rows if pairs_with_rows > 0 else 0
        )
        estimated_rows = int(TOTAL_INTERACTION_PAIRS * hit_rate * avg_rows_per_hit)
        print(f"\n  --- Extrapolation from {stats.pairs_processed} sampled pairs ---")
        print(f"  Formulation-resolved rate              : {100*hit_rate:>10.1f}%")
        print(f"  Avg rows per resolved pair             : {avg_rows_per_hit:>12.2f}")
        print(f"  Est. total pairs with formulations     : {estimated_total:>12,}")
        print(f"  Est. total rows on full run            : {estimated_rows:>12,}")

    print("=" * 60)

    if stats.error_details:
        print("\n  Error details (first 20):")
        for detail in stats.error_details[:20]:
            print(f"    {detail}")


if __name__ == "__main__":
    main()
