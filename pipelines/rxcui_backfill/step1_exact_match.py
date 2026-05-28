"""
step1_exact_match.py — Step 1: exact lowercase match of ingredient_name_norm → rxnconso.str
"""

import time
import logging
from typing import Dict, List, Tuple

import psycopg2

from utils import (
    setup_logger, batch_update_rxcui, batch_insert_audit, fmt_ist, now_ist
)
from config import DEFAULT_BATCH_SIZE


def run_step1(
    conn: psycopg2.extensions.connection,
    rxconso_lookup: Dict[str, Tuple[str, str]],   # {lower_str: (rxcui, tty)}
    target_rows: List[Tuple],                      # [(id, brand_id, name_norm), ...]
    run_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    skip_audit: bool = False,
) -> Tuple[int, List[Tuple]]:
    """
    Returns (resolved_count, unresolved_rows).
    unresolved_rows = list of (id, brand_id, name_norm) not matched in this step.
    """
    logger = setup_logger("step1", "step1_exact_match.log")
    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}Step 1 — Exact Match started at {fmt_ist()}")
    logger.info(f"Candidate rows: {len(target_rows)}")

    t0 = time.time()

    updates: List[Tuple] = []       # (rxcui, confidence, id)
    audit_rows: List[Tuple] = []    # full audit tuples
    unresolved: List[Tuple] = []

    # Per-name dedup warning tracking
    name_to_rxcui: Dict[str, str] = {}
    warned_names = set()

    for row_id, brand_id, name_norm in target_rows:
        key = name_norm.strip().lower()
        if key in rxconso_lookup:
            rxcui, tty = rxconso_lookup[key]

            # Safety check: same ingredient_name_norm → different RXCUI?
            if key in name_to_rxcui and name_to_rxcui[key] != rxcui and key not in warned_names:
                logger.warning(
                    f"[WARN] Duplicate RXCUI conflict for ingredient '{name_norm}': "
                    f"prev={name_to_rxcui[key]} vs new={rxcui}"
                )
                warned_names.add(key)
            name_to_rxcui[key] = rxcui

            updates.append((rxcui, "exact", row_id))
            audit_rows.append((
                row_id, brand_id, name_norm,
                None, None,          # stripped_name, synonym_used
                key, rxcui, tty,
                1, "exact", run_id,
            ))
            prefix = "[DRY-RUN] " if dry_run else ""
            logger.debug(
                f"{prefix}{fmt_ist()}\t{row_id}\t{brand_id}\t{name_norm}\t{key}\t{rxcui}\t{tty}"
            )
        else:
            unresolved.append((row_id, brand_id, name_norm))

    resolved_count = batch_update_rxcui(conn, updates, batch_size, dry_run, logger)
    batch_insert_audit(conn, audit_rows, batch_size, dry_run, skip_audit, logger)

    elapsed = time.time() - t0
    pct = resolved_count / len(target_rows) * 100 if target_rows else 0
    msg = (
        f"{'[DRY-RUN] ' if dry_run else ''}"
        f"Step 1 complete: {resolved_count} resolved out of {len(target_rows)} total "
        f"({pct:.1f}%) in {elapsed:.1f}s"
    )
    logger.info(msg)
    print(msg)

    return resolved_count, unresolved
