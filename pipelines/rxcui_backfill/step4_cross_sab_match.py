"""
step4_cross_sab_match.py — WRONG_SAB resolution.

Many drugs exist in rxnconso under non-RXNORM sources (DrugBank, SNOMEDCT, NDDF etc.)
and share the same rxcui as a valid RXNORM IN/PIN entry.

Strategy: load a lookup {lower_str: (rxcui, tty)} built by joining non-RXNORM entries
to their RXNORM IN/PIN counterpart via shared rxcui. Then match unresolved names.
"""

import time
from typing import Dict, List, Tuple

import psycopg2
import psycopg2.extras

from utils import setup_logger, batch_update_rxcui, batch_insert_audit, fmt_ist
from config import DEFAULT_BATCH_SIZE

CROSS_SAB_QUERY = """
    SELECT LOWER(r1.str) AS str_lower, r2.rxcui, r2.tty, r1.sab AS source_sab
    FROM public.rxnconso r1
    JOIN public.rxnconso r2 ON r1.rxcui = r2.rxcui
    WHERE r1.sab != 'RXNORM'
      AND r1.suppress = 'N'
      AND r2.sab = 'RXNORM'
      AND r2.tty IN ('IN', 'PIN')
      AND r2.suppress = 'N'
"""


def load_cross_sab_lookup(conn) -> Dict[str, Tuple[str, str]]:
    """
    Build {lower_str: (rxcui, tty)} from non-RXNORM entries whose rxcui
    maps to a valid RXNORM IN/PIN concept.
    Prefer IN over PIN; break ties by lowest rxcui.
    """
    print("Step 4 pre-load: building cross-SAB lookup...")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(CROSS_SAB_QUERY)
        rows = cur.fetchall()

    lookup: Dict[str, Tuple[str, str]] = {}
    for str_lower, rxcui, tty, _ in rows:
        if str_lower not in lookup:
            lookup[str_lower] = (rxcui, tty)
        else:
            existing_rxcui, existing_tty = lookup[str_lower]
            if tty == "IN" and existing_tty != "IN":
                lookup[str_lower] = (rxcui, tty)
            elif tty == existing_tty and rxcui < existing_rxcui:
                lookup[str_lower] = (rxcui, tty)

    print(f"  {len(lookup)} cross-SAB entries loaded in {time.time()-t0:.1f}s")
    return lookup


def run_step4(
    conn: psycopg2.extensions.connection,
    cross_sab_lookup: Dict[str, Tuple[str, str]],
    unresolved_rows: List[Tuple],
    run_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    skip_audit: bool = False,
) -> Tuple[int, List[Tuple]]:
    """
    Returns (resolved_count, still_unresolved_rows).
    """
    logger = setup_logger("step4", "step4_cross_sab_match.log")
    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}Step 4 — Cross-SAB Match started at {fmt_ist()}")
    logger.info(f"Candidate rows (post Step 3): {len(unresolved_rows)}")

    t0 = time.time()

    updates: List[Tuple] = []
    audit_rows: List[Tuple] = []
    still_unresolved: List[Tuple] = []

    name_to_rxcui: Dict[str, str] = {}
    warned_names = set()

    for row_id, brand_id, name_norm in unresolved_rows:
        key = name_norm.strip().lower()
        if key in cross_sab_lookup:
            rxcui, tty = cross_sab_lookup[key]

            if key in name_to_rxcui and name_to_rxcui[key] != rxcui and key not in warned_names:
                logger.warning(
                    f"[WARN] Duplicate RXCUI conflict for '{name_norm}': "
                    f"prev={name_to_rxcui[key]} vs new={rxcui}"
                )
                warned_names.add(key)
            name_to_rxcui[key] = rxcui

            updates.append((rxcui, "cross_sab", row_id))
            audit_rows.append((
                row_id, brand_id, name_norm,
                None, None,
                key, rxcui, tty,
                4, "cross_sab", run_id,
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
        f"Step 4 complete: {resolved_count} resolved ({pct:.1f}% of remaining) in {elapsed:.1f}s"
    )
    logger.info(msg)
    print(msg)

    return resolved_count, still_unresolved
