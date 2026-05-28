"""
step5_tty_traverse.py — WRONG_TTY resolution.

Some drugs exist in rxnconso under RXNORM but as SY (synonym) or TMSY (tallman synonym)
instead of IN/PIN. Their rxcui IS the same as the IN/PIN concept — we just never
loaded them because we filtered on tty IN ('IN','PIN').

Strategy: build a lookup {lower_str: (rxcui, tty)} for RXNORM SY/TMSY entries
whose rxcui HAS a valid RXNORM IN/PIN entry. The rxcui is already correct —
just use it directly.
"""

import time
from typing import Dict, List, Tuple

import psycopg2
import psycopg2.extras

from utils import setup_logger, batch_update_rxcui, batch_insert_audit, fmt_ist
from config import DEFAULT_BATCH_SIZE

TTY_TRAVERSE_QUERY = """
    SELECT LOWER(r1.str) AS str_lower, r2.rxcui, r2.tty
    FROM public.rxnconso r1
    JOIN public.rxnconso r2 ON r1.rxcui = r2.rxcui
    WHERE r1.sab = 'RXNORM'
      AND r1.tty NOT IN ('IN', 'PIN')
      AND r1.suppress = 'N'
      AND r2.sab = 'RXNORM'
      AND r2.tty IN ('IN', 'PIN')
      AND r2.suppress = 'N'
"""


def load_tty_traverse_lookup(conn) -> Dict[str, Tuple[str, str]]:
    """
    Build {lower_str: (rxcui, tty)} for RXNORM SY/TMSY entries that share
    a rxcui with a valid RXNORM IN/PIN concept.
    """
    print("Step 5 pre-load: building TTY-traverse lookup...")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(TTY_TRAVERSE_QUERY)
        rows = cur.fetchall()

    lookup: Dict[str, Tuple[str, str]] = {}
    for str_lower, rxcui, tty in rows:
        if str_lower not in lookup:
            lookup[str_lower] = (rxcui, tty)
        else:
            existing_rxcui, existing_tty = lookup[str_lower]
            if tty == "IN" and existing_tty != "IN":
                lookup[str_lower] = (rxcui, tty)
            elif tty == existing_tty and rxcui < existing_rxcui:
                lookup[str_lower] = (rxcui, tty)

    print(f"  {len(lookup)} TTY-traverse entries loaded in {time.time()-t0:.1f}s")
    return lookup


def run_step5(
    conn: psycopg2.extensions.connection,
    tty_traverse_lookup: Dict[str, Tuple[str, str]],
    unresolved_rows: List[Tuple],
    run_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    skip_audit: bool = False,
) -> Tuple[int, List[Tuple]]:
    """
    Returns (resolved_count, still_unresolved_rows).
    """
    logger = setup_logger("step5", "step5_tty_traverse_match.log")
    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}Step 5 — TTY-Traverse Match started at {fmt_ist()}")
    logger.info(f"Candidate rows (post Step 4): {len(unresolved_rows)}")

    t0 = time.time()

    updates: List[Tuple] = []
    audit_rows: List[Tuple] = []
    still_unresolved: List[Tuple] = []

    name_to_rxcui: Dict[str, str] = {}
    warned_names = set()

    for row_id, brand_id, name_norm in unresolved_rows:
        key = name_norm.strip().lower()
        if key in tty_traverse_lookup:
            rxcui, tty = tty_traverse_lookup[key]

            if key in name_to_rxcui and name_to_rxcui[key] != rxcui and key not in warned_names:
                logger.warning(
                    f"[WARN] Duplicate RXCUI conflict for '{name_norm}': "
                    f"prev={name_to_rxcui[key]} vs new={rxcui}"
                )
                warned_names.add(key)
            name_to_rxcui[key] = rxcui

            updates.append((rxcui, "tty_traverse", row_id))
            audit_rows.append((
                row_id, brand_id, name_norm,
                None, None,
                key, rxcui, tty,
                5, "tty_traverse", run_id,
            ))
            prefix = "[DRY-RUN] " if dry_run else ""
            logger.debug(
                f"{prefix}{fmt_ist()}\t{row_id}\t{brand_id}\t{name_norm}\t{key}\t{rxcui}\t{tty}"
            )
        else:
            still_unresolved.append((row_id, brand_id, name_norm))

    resolved_count = batch_update_rxcui(conn, updates, batch_size, dry_run, logger)
    batch_insert_audit(conn, audit_rows, batch_size, dry_run, skip_audit, logger)

    elapsed = time.time() - t0
    pct = resolved_count / len(unresolved_rows) * 100 if unresolved_rows else 0
    msg = (
        f"{'[DRY-RUN] ' if dry_run else ''}"
        f"Step 5 complete: {resolved_count} resolved ({pct:.1f}% of remaining) in {elapsed:.1f}s"
    )
    logger.info(msg)
    print(msg)

    unresolved_count = len(still_unresolved)
    msg2 = f"Unresolved after all steps: {unresolved_count} rows"
    logger.info(msg2)
    print(msg2)

    return resolved_count, still_unresolved
