"""
step3_salt_strip_match.py — Step 3: salt-stripped name → retry exact + synonym match.
"""

import time
from typing import Dict, List, Tuple, Optional

import psycopg2

from utils import (
    setup_logger, batch_update_rxcui, batch_insert_audit, fmt_ist
)
from config import DEFAULT_BATCH_SIZE, MIN_STRIPPED_LENGTH
from salt_patterns import SALT_PATTERN, COMPOUND_SALT_PATTERNS


def _strip_salt(original: str) -> Optional[str]:
    """
    Apply COMPOUND_SALT_PATTERNS (more specific) first, then SALT_PATTERN.
    Returns stripped string, or None if nothing changed or result is too short.
    """
    stripped = original
    for pattern in COMPOUND_SALT_PATTERNS:
        result = pattern.sub("", stripped).strip()
        if result != stripped:
            stripped = result
            break

    if stripped == original:
        stripped = SALT_PATTERN.sub("", original).strip()

    if not stripped or stripped.lower() == original.lower():
        return None
    if len(stripped) < MIN_STRIPPED_LENGTH:
        return None
    return stripped


def run_step3(
    conn: psycopg2.extensions.connection,
    rxconso_lookup: Dict[str, Tuple[str, str]],
    synonym_reverse: Dict[str, List[str]],
    unresolved_rows: List[Tuple],
    run_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    skip_audit: bool = False,
) -> Tuple[int, List[Tuple]]:
    """
    Returns (step3_resolved_count, still_unresolved_rows).
    """
    logger = setup_logger("step3", "step3_salt_strip_match.log")
    unresolved_logger = setup_logger("unresolved", "unresolved.log")

    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}Step 3 — Salt-Strip Match started at {fmt_ist()}")
    logger.info(f"Candidate rows (post Step 2): {len(unresolved_rows)}")

    t0 = time.time()

    updates: List[Tuple] = []
    audit_rows: List[Tuple] = []
    still_unresolved: List[Tuple] = []

    name_to_rxcui: Dict[str, str] = {}
    warned_names = set()

    for row_id, brand_id, name_norm in unresolved_rows:
        original = name_norm.strip()
        stripped = _strip_salt(original)

        if stripped is None:
            # Cannot strip — log as unresolved immediately
            prefix = "[DRY-RUN] " if dry_run else ""
            unresolved_logger.debug(
                f"{prefix}{fmt_ist()}\t{row_id}\t{brand_id}\t{name_norm}\t"
            )
            still_unresolved.append((row_id, brand_id, name_norm))
            continue

        key = stripped.lower()
        matched = False
        rxcui = tty = synonym_used = matched_str = confidence = None

        # Sub-step A: exact match with stripped name
        if key in rxconso_lookup:
            rxcui, tty = rxconso_lookup[key]
            matched_str = key
            confidence = "salt_strip_exact"
            synonym_used = None
            matched = True

        # Sub-step B: synonym match with stripped name
        if not matched and key in synonym_reverse:
            for synonym in synonym_reverse[key]:
                if synonym in rxconso_lookup:
                    rxcui, tty = rxconso_lookup[synonym]
                    matched_str = synonym
                    confidence = "salt_strip_synonym"
                    synonym_used = synonym
                    matched = True
                    break

        if matched:
            norm_key = original.lower()
            if norm_key in name_to_rxcui and name_to_rxcui[norm_key] != rxcui and norm_key not in warned_names:
                logger.warning(
                    f"[WARN] Duplicate RXCUI conflict for '{name_norm}': "
                    f"prev={name_to_rxcui[norm_key]} vs new={rxcui}"
                )
                warned_names.add(norm_key)
            name_to_rxcui[norm_key] = rxcui

            updates.append((rxcui, confidence, row_id))
            audit_rows.append((
                row_id, brand_id, name_norm,
                stripped, synonym_used,
                matched_str, rxcui, tty,
                3, confidence, run_id,
            ))
            prefix = "[DRY-RUN] " if dry_run else ""
            logger.debug(
                f"{prefix}{fmt_ist()}\t{row_id}\t{brand_id}\t{name_norm}\t"
                f"{stripped}\t{synonym_used or ''}\t{matched_str}\t{rxcui}\t{tty}\t{confidence}"
            )
        else:
            prefix = "[DRY-RUN] " if dry_run else ""
            unresolved_logger.debug(
                f"{prefix}{fmt_ist()}\t{row_id}\t{brand_id}\t{name_norm}\t{stripped}"
            )
            still_unresolved.append((row_id, brand_id, name_norm))

    resolved_count = batch_update_rxcui(conn, updates, batch_size, dry_run, logger)
    batch_insert_audit(conn, audit_rows, batch_size, dry_run, skip_audit, logger)

    elapsed = time.time() - t0
    pct = resolved_count / len(unresolved_rows) * 100 if unresolved_rows else 0
    msg = (
        f"{'[DRY-RUN] ' if dry_run else ''}"
        f"Step 3 complete: {resolved_count} resolved ({pct:.1f}% of remaining) in {elapsed:.1f}s"
    )
    logger.info(msg)
    print(msg)

    unresolved_count = len(still_unresolved)
    msg2 = f"Unresolved after all steps: {unresolved_count} rows"
    logger.info(msg2)
    print(msg2)

    return resolved_count, still_unresolved
