#!/usr/bin/env python3
"""
run_pipeline.py — Orchestrates all 5 RXCUI backfill steps.

Usage:
    python run_pipeline.py [--dry-run] [--limit N] [--step {1,2,3,4,5,all}]
                           [--batch-size N] [--password TEXT] [--skip-audit]
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Allow importing sibling modules
sys.path.insert(0, str(Path(__file__).parent))

# Load .env from ~/cdss/.env if present (before importing config)
_env_path = Path(__file__).parent.parent.parent / ".env"
if _env_path.exists():
    # Manual parse to avoid requiring python-dotenv
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k not in os.environ:
                    os.environ[_k] = _v

import psycopg2
import psycopg2.extras

from config import (
    SYNONYMS_JSON, LOGS_DIR, DEFAULT_BATCH_SIZE,
    RXNCONSO_QUERY, TARGET_ROWS_QUERY,
)
from utils import connect_db, setup_logger, fmt_ist, now_ist
from step1_exact_match import run_step1
from step2_synonym_match import run_step2
from step3_salt_strip_match import run_step3
from step4_cross_sab_match import run_step4, load_cross_sab_lookup
from step5_tty_traverse import run_step5, load_tty_traverse_lookup


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="RXCUI Backfill Pipeline")
    p.add_argument("--dry-run", action="store_true",
                   help="Run matching logic without committing any DB changes.")
    p.add_argument("--limit", type=int, default=None,
                   help="Process only first N rows (for testing).")
    p.add_argument("--step", choices=["1", "2", "3", "4", "5", "all"], default="all",
                   help="Run a specific step or all. Default: all.")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                   help=f"Commit batch size. Default: {DEFAULT_BATCH_SIZE}.")
    p.add_argument("--password", type=str, default=None,
                   help="DB password (overrides .env).")
    p.add_argument("--skip-audit", action="store_true",
                   help="Skip writing to audit table (faster for testing).")
    return p.parse_args()


# ── Pre-load phase ────────────────────────────────────────────────────────────

def load_rxnconso(conn: psycopg2.extensions.connection) -> Dict[str, Tuple[str, str]]:
    """
    Load filtered rxnconso into {lower_str: (rxcui, tty)}.
    Prefer tty='IN' over 'PIN'; break ties by lowest rxcui.
    """
    print("Pre-load: querying rxnconso (IN/PIN, RXNORM, suppress=N)...")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(RXNCONSO_QUERY)
        rows = cur.fetchall()

    lookup: Dict[str, Tuple[str, str]] = {}
    # rows: (str_lower, rxcui, tty)
    for str_lower, rxcui, tty in rows:
        if str_lower not in lookup:
            lookup[str_lower] = (rxcui, tty)
        else:
            existing_rxcui, existing_tty = lookup[str_lower]
            # Prefer IN > PIN
            if tty == "IN" and existing_tty != "IN":
                lookup[str_lower] = (rxcui, tty)
            elif tty == existing_tty:
                # Tie-break: lowest rxcui (string comparison works for numeric-like IDs)
                if rxcui < existing_rxcui:
                    lookup[str_lower] = (rxcui, tty)

    print(f"Pre-load: {len(lookup)} unique RxConso IN/PIN entries loaded in {time.time()-t0:.1f}s")
    return lookup


def load_synonyms(path: Path) -> Dict[str, List[str]]:
    """
    Build reverse synonym lookup: {name_lower: [other_synonyms...]}.
    """
    print(f"Pre-load: loading synonyms from {path}...")
    if not path.exists():
        print(f"[WARN] synonyms.json not found at {path}. Synonym matching will be skipped.")
        return {}

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    reverse: Dict[str, List[str]] = {}
    for group in data.get("synonyms", []):
        members = [s.lower() for s in group.get("synonyms", [])]
        for i, member in enumerate(members):
            others = members[:i] + members[i+1:]
            if member not in reverse:
                reverse[member] = []
            for other in others:
                if other not in reverse[member]:
                    reverse[member].append(other)

    print(f"Pre-load: {len(data.get('synonyms', []))} synonym groups → {len(reverse)} reverse entries")
    return reverse


def load_target_rows(
    conn: psycopg2.extensions.connection,
    limit: int = None,
) -> List[Tuple]:
    """
    Load all indian_brand_ingredient rows with rxcui_in IS NULL.
    Optionally limited to `limit` rows.
    """
    print("Pre-load: querying target rows (rxcui_in IS NULL)...")
    t0 = time.time()
    query = TARGET_ROWS_QUERY
    if limit:
        query += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    print(f"Pre-load: {len(rows)} target rows loaded in {time.time()-t0:.1f}s")
    return rows  # [(id, indian_brand_id, ingredient_name_norm), ...]


# ── Validation queries ────────────────────────────────────────────────────────

VALIDATION_QUERIES = [
    (
        "Resolution summary by confidence",
        """
        SELECT match_confidence, COUNT(*)
        FROM drugdb.indian_brand_ingredient
        WHERE rxcui_in IS NOT NULL
        GROUP BY match_confidence
        ORDER BY COUNT(*) DESC
        """
    ),
    (
        "Still unresolved (rxcui_in IS NULL)",
        "SELECT COUNT(*) FROM drugdb.indian_brand_ingredient WHERE rxcui_in IS NULL"
    ),
    (
        "Top 20 unresolved ingredient names",
        """
        SELECT ingredient_name_norm, COUNT(*) AS cnt
        FROM drugdb.indian_brand_ingredient
        WHERE rxcui_in IS NULL
        GROUP BY ingredient_name_norm
        ORDER BY cnt DESC
        LIMIT 20
        """
    ),
    (
        "Audit table rows per step (this run)",
        None,  # handled dynamically with run_id
    ),
    (
        "Ingredients with multiple distinct RXCUIs (data quality check)",
        """
        SELECT ingredient_name_norm, COUNT(DISTINCT rxcui_in) AS distinct_rxcuis
        FROM drugdb.indian_brand_ingredient
        WHERE rxcui_in IS NOT NULL
        GROUP BY ingredient_name_norm
        HAVING COUNT(DISTINCT rxcui_in) > 1
        LIMIT 20
        """
    ),
]


def run_validation(conn: psycopg2.extensions.connection, run_id: str, summary_logger) -> None:
    print("\n=== POST-RUN VALIDATION ===\n")
    with conn.cursor() as cur:
        for label, query in VALIDATION_QUERIES:
            if query is None:
                # Audit per-step query with run_id
                query = f"""
                SELECT resolution_step, COUNT(*)
                FROM drugdb.rxcui_resolution_audit
                WHERE pipeline_run_id = '{run_id}'
                GROUP BY resolution_step
                ORDER BY resolution_step
                """
            try:
                cur.execute(query)
                rows = cur.fetchall()
                header = f"\n--- {label} ---"
                print(header)
                summary_logger.info(header)
                for row in rows:
                    line = "\t".join(str(c) for c in row)
                    print(line)
                    summary_logger.info(line)
            except psycopg2.Error as e:
                msg = f"[ERROR] Validation query '{label}' failed: {e}"
                print(msg)
                summary_logger.error(msg)
                conn.rollback()


# ── Summary logger ────────────────────────────────────────────────────────────

def write_summary(
    summary_logger,
    run_id: str,
    started: datetime,
    finished: datetime,
    preload_stats: dict,
    step_stats: dict,
    total_rows: int,
    dry_run: bool,
) -> None:
    elapsed = (finished - started).total_seconds()
    lines = [
        f"Pipeline Run ID: {run_id}",
        f"Started:         {fmt_ist(started)}",
        f"Finished:        {fmt_ist(finished)}",
        f"Total Duration:  {elapsed:.1f}s",
        f"Dry Run:         {dry_run}",
        "",
        "Pre-load:",
        f"  RxConso IN/PIN entries loaded: {preload_stats.get('rxconso_count', 'N/A')}",
        f"  Synonym groups loaded:         {preload_stats.get('synonym_groups', 'N/A')}",
        f"  Target rows (rxcui_in IS NULL): {total_rows}",
    ]

    cumulative = 0
    step_labels = {
        1: "Exact Match",
        2: "Synonym Match",
        3: "Salt-Strip Match",
        4: "Cross-SAB Match",
        5: "TTY-Traverse Match",
    }
    for step_num in [1, 2, 3, 4, 5]:
        stats = step_stats.get(step_num)
        if stats is None:
            continue
        cumulative += stats["resolved"]
        cum_pct = cumulative / total_rows * 100 if total_rows else 0
        pct = stats["resolved"] / max(stats["candidates"], 1) * 100
        step_label = step_labels[step_num]
        lines += [
            "",
            f"Step {step_num} — {step_label}:",
            f"  Candidates:          {stats['candidates']}",
            f"  Resolved:            {stats['resolved']} ({pct:.1f}% of candidates)",
            f"  Cumulative resolved: {cumulative} ({cum_pct:.1f}% of total)",
            f"  Duration:            {stats['duration']:.1f}s",
        ]

    unresolved = total_rows - cumulative
    unresolved_pct = unresolved / total_rows * 100 if total_rows else 0
    lines += [
        "",
        f"Unresolved: {unresolved} ({unresolved_pct:.1f}%)",
    ]

    for line in lines:
        print(line)
        summary_logger.info(line)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    run_id = str(uuid.uuid4())
    started = now_ist()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    summary_logger = setup_logger("summary", "pipeline_summary.log")

    print(f"\n{'='*60}")
    print(f"RXCUI Backfill Pipeline")
    print(f"Run ID:   {run_id}")
    print(f"Started:  {fmt_ist(started)}")
    print(f"Dry Run:  {args.dry_run}")
    print(f"Step:     {args.step}")
    print(f"Limit:    {args.limit or 'all'}")
    print(f"Batch:    {args.batch_size}")
    print(f"{'='*60}\n")

    conn = connect_db(args.password)
    step_num = args.step

    # ── Pre-load phase ─────────────────────────────────────────────────────────
    t_preload = time.time()
    rxconso_lookup = load_rxnconso(conn)
    synonym_reverse = load_synonyms(SYNONYMS_JSON)
    cross_sab_lookup = load_cross_sab_lookup(conn) if step_num in ("4", "all") else {}
    tty_traverse_lookup = load_tty_traverse_lookup(conn) if step_num in ("5", "all") else {}
    target_rows = load_target_rows(conn, args.limit)
    print(f"Pre-load complete in {time.time()-t_preload:.1f}s\n")

    preload_stats = {
        "rxconso_count": len(rxconso_lookup),
        "synonym_groups": len(set(
            frozenset([k] + v) for k, v in synonym_reverse.items()
        )),
    }

    total_rows = len(target_rows)
    step_stats: dict = {}

    if step_num in ("1", "all"):
        t0 = time.time()
        resolved1, unresolved_after1 = run_step1(
            conn, rxconso_lookup, target_rows,
            run_id, args.batch_size, args.dry_run, args.skip_audit,
        )
        step_stats[1] = {"candidates": total_rows, "resolved": resolved1, "duration": time.time() - t0}
        cum = resolved1
        print(f"Cumulative after Step 1: {cum}/{total_rows} ({cum/total_rows*100:.1f}%)\n")
    else:
        unresolved_after1 = target_rows

    if step_num in ("2", "all"):
        t0 = time.time()
        resolved2, unresolved_after2 = run_step2(
            conn, rxconso_lookup, synonym_reverse, unresolved_after1,
            run_id, args.batch_size, args.dry_run, args.skip_audit,
        )
        step_stats[2] = {"candidates": len(unresolved_after1), "resolved": resolved2, "duration": time.time() - t0}
        cum = sum(s["resolved"] for s in step_stats.values())
        print(f"Cumulative after Step 2: {cum}/{total_rows} ({cum/total_rows*100:.1f}%)\n")
    else:
        unresolved_after2 = unresolved_after1

    if step_num in ("3", "all"):
        t0 = time.time()
        resolved3, unresolved_after3 = run_step3(
            conn, rxconso_lookup, synonym_reverse, unresolved_after2,
            run_id, args.batch_size, args.dry_run, args.skip_audit,
        )
        step_stats[3] = {"candidates": len(unresolved_after2), "resolved": resolved3, "duration": time.time() - t0}
        cum = sum(s["resolved"] for s in step_stats.values())
        print(f"Cumulative after Step 3: {cum}/{total_rows} ({cum/total_rows*100:.1f}%)\n")
    else:
        unresolved_after3 = unresolved_after2

    if step_num in ("4", "all"):
        t0 = time.time()
        resolved4, unresolved_after4 = run_step4(
            conn, cross_sab_lookup, unresolved_after3,
            run_id, args.batch_size, args.dry_run, args.skip_audit,
        )
        step_stats[4] = {"candidates": len(unresolved_after3), "resolved": resolved4, "duration": time.time() - t0}
        cum = sum(s["resolved"] for s in step_stats.values())
        print(f"Cumulative after Step 4: {cum}/{total_rows} ({cum/total_rows*100:.1f}%)\n")
    else:
        unresolved_after4 = unresolved_after3

    if step_num in ("5", "all"):
        t0 = time.time()
        resolved5, still_unresolved = run_step5(
            conn, tty_traverse_lookup, unresolved_after4,
            run_id, args.batch_size, args.dry_run, args.skip_audit,
        )
        step_stats[5] = {"candidates": len(unresolved_after4), "resolved": resolved5, "duration": time.time() - t0}
        cum = sum(s["resolved"] for s in step_stats.values())
        print(f"Cumulative after Step 5: {cum}/{total_rows} ({cum/total_rows*100:.1f}%)")
        print(f"Unresolved: {len(still_unresolved)} rows\n")

    finished = now_ist()
    write_summary(
        summary_logger, run_id, started, finished,
        preload_stats, step_stats, total_rows, args.dry_run,
    )

    # ── Validation (skip in dry-run) ───────────────────────────────────────────
    if not args.dry_run and step_num == "all":
        run_validation(conn, run_id, summary_logger)

    conn.close()
    print(f"\nPipeline finished. Logs in: {LOGS_DIR}")


if __name__ == "__main__":
    main()
