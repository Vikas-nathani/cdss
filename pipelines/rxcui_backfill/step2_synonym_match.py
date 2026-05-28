"""
step2_synonym_match.py — Step 2: synonym-expanded name → rxnconso.str
"""

import time
from typing import Dict, List, Tuple

import psycopg2

from utils import (
    setup_logger, batch_update_rxcui, batch_insert_audit, fmt_ist
)
from config import DEFAULT_BATCH_SIZE


def run_step2(
    conn: psycopg2.extensions.connection,
    rxconso_lookup: Dict[str, Tuple[str, str]],       # {lower_str: (rxcui, tty)}
    synonym_reverse: Dict[str, List[str]],             # {name_lower: [other_synonyms...]}
    unresolved_rows: List[Tuple],                      # [(id, brand_id, name_norm), ...]
    run_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    skip_audit: bool = False,
) -> Tuple[int, List[Tuple]]:
    """
    Returns (step2_resolved_count, still_unresolved_rows).
    """
    logger = setup_logger("step2", "step2_synonym_match.log")
    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}Step 2 — Synonym Match started at {fmt_ist()}")
    logger.info(f"Candidate rows (post Step 1): {len(unresolved_rows)}")

    # Warn if synonym group members map to different RXCUIs in rxnconso
    _warn_synonym_conflicts(synonym_reverse, rxconso_lookup, logger)

    t0 = time.time()

    updates: List[Tuple] = []
    audit_rows: List[Tuple] = []
    still_unresolved: List[Tuple] = []

    name_to_rxcui: Dict[str, str] = {}
    warned_names = set()

    for row_id, brand_id, name_norm in unresolved_rows:
        key = name_norm.strip().lower()
        matched = False
        if key in synonym_reverse:
            for synonym in synonym_reverse[key]:
                if synonym in rxconso_lookup:
                    rxcui, tty = rxconso_lookup[synonym]

                    if key in name_to_rxcui and name_to_rxcui[key] != rxcui and key not in warned_names:
                        logger.warning(
                            f"[WARN] Duplicate RXCUI conflict for '{name_norm}': "
                            f"prev={name_to_rxcui[key]} vs new={rxcui}"
                        )
                        warned_names.add(key)
                    name_to_rxcui[key] = rxcui

                    updates.append((rxcui, "synonym", row_id))
                    audit_rows.append((
                        row_id, brand_id, name_norm,
                        None, synonym,
                        synonym, rxcui, tty,
                        2, "synonym", run_id,
                    ))
                    prefix = "[DRY-RUN] " if dry_run else ""
                    logger.debug(
                        f"{prefix}{fmt_ist()}\t{row_id}\t{brand_id}\t"
                        f"{name_norm}\t{synonym}\t{synonym}\t{rxcui}\t{tty}"
                    )
                    matched = True
                    break

        if not matched:
            still_unresolved.append((row_id, brand_id, name_norm))

    resolved_count = batch_update_rxcui(conn, updates, batch_size, dry_run, logger)
    batch_insert_audit(conn, audit_rows, batch_size, dry_run, skip_audit, logger)

    elapsed = time.time() - t0
    pct = resolved_count / len(unresolved_rows) * 100 if unresolved_rows else 0
    msg = (
        f"{'[DRY-RUN] ' if dry_run else ''}"
        f"Step 2 complete: {resolved_count} resolved ({pct:.1f}% of remaining) in {elapsed:.1f}s"
    )
    logger.info(msg)
    print(msg)

    return resolved_count, still_unresolved


def _warn_synonym_conflicts(
    synonym_reverse: Dict[str, List[str]],
    rxconso_lookup: Dict[str, Tuple[str, str]],
    logger,
) -> None:
    """
    Log a WARNING if a synonym group has members resolving to different RXCUIs.
    This catches data quality issues like aceclofenac/diclofenac being grouped.
    """
    # Build group detection: gather all unique names per synonym group
    # synonym_reverse maps each name → its siblings; iterate unique groups
    seen_groups = set()
    for name, siblings in synonym_reverse.items():
        group_key = frozenset([name] + siblings)
        if group_key in seen_groups:
            continue
        seen_groups.add(group_key)

        rxcuis_in_group = {}
        for member in group_key:
            if member in rxconso_lookup:
                rxcuis_in_group[member] = rxconso_lookup[member][0]

        unique_rxcuis = set(rxcuis_in_group.values())
        if len(unique_rxcuis) > 1:
            logger.warning(
                f"[WARN] Synonym group {sorted(group_key)} maps to "
                f"multiple RXCUIs: {rxcuis_in_group}"
            )
