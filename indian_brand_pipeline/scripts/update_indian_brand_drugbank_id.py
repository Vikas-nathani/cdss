#!/usr/bin/env python3
"""
Update drugdb.indian_brand_ingredient.drugbank_id (currently NULL) by matching
ingredient_name_raw / ingredient_name_norm against drugdb.ingredients and
drugdb.ingredient_synonyms using 4-tier logic.

Execution is two-phase:
  Phase 1  — dry-run matching with full statistics report (no DB writes).
  Phase 2  — apply updates in batches after user confirms.

Matching tiers (applied in order, first match wins):
  Tier 1  Exact match against ingredients.name        (case-insensitive)
  Tier 2  Exact match against ingredient_synonyms     (case-insensitive)
  Tier 3  Prefix match against ingredients.name       (shortest wins)
  Tier 4  Prefix match against ingredient_synonyms    (shortest wins)

For each record, ingredient_name_raw is tried first; if no match is found the
same 4-tier logic is repeated for ingredient_name_norm.
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────

@dataclass
class Stats:
    total_records: int = 0
    tier1_count: int = 0
    tier2_count: int = 0
    tier3_count: int = 0
    tier4_count: int = 0
    unmatched_count: int = 0
    update_errors: int = 0
    error_details: list = field(default_factory=list)

    @property
    def matched_count(self) -> int:
        return self.tier1_count + self.tier2_count + self.tier3_count + self.tier4_count

    @property
    def match_rate_pct(self) -> float:
        if self.total_records == 0:
            return 0.0
        return round(self.matched_count / self.total_records * 100, 1)


# ─────────────────────────────────────────────
# Database helpers
# ─────────────────────────────────────────────

def get_connection(host: str, dbname: str, user: str, password: str, port: int = 5432):
    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        connect_timeout=30,
    )


def load_ingredients(cur, log: logging.Logger) -> tuple[dict, list]:
    """
    Load all rows from drugdb.ingredients into memory.

    Returns:
        ing_exact  — dict  lower(name) → {id, drugbank_id, name}
        ing_list   — list  [{name_lower, drugbank_id, name, id}, ...]  sorted for prefix scanning
    """
    log.info("Loading drugdb.ingredients into memory …")
    cur.execute("""
        SELECT id::text AS id, name, drugbank_id
        FROM drugdb.ingredients
        WHERE name IS NOT NULL AND trim(name) <> ''
    """)
    rows = cur.fetchall()

    ing_exact: dict[str, dict] = {}
    ing_list: list[dict] = []

    for row in rows:
        name_lower = row["name"].lower()
        entry = {
            "id": row["id"],
            "drugbank_id": row["drugbank_id"],
            "name": row["name"],
            "name_lower": name_lower,
        }
        # Exact index: last write wins for duplicate lower names (rare)
        ing_exact[name_lower] = entry
        ing_list.append(entry)

    # Sort ascending by name length so shortest-name prefix winner is found first
    ing_list.sort(key=lambda e: len(e["name"]))

    log.info("Loaded %d ingredient rows (%d distinct lower-name keys)", len(ing_list), len(ing_exact))
    return ing_exact, ing_list


def load_synonyms(cur, log: logging.Logger) -> tuple[dict, list]:
    """
    Load all rows from drugdb.ingredient_synonyms joined to drugdb.ingredients so
    drugbank_id is immediately available.

    Returns:
        syn_exact — dict  lower(synonym) → {ingredient_id, drugbank_id, ingredient_name, synonym}
        syn_list  — list  [{synonym_lower, drugbank_id, ingredient_name, ingredient_id, synonym}, ...]
    """
    log.info("Loading drugdb.ingredient_synonyms (joined to ingredients) into memory …")
    cur.execute("""
        SELECT
            s.id::text          AS ingredient_id,
            s.synonym,
            i.drugbank_id,
            i.name              AS ingredient_name
        FROM drugdb.ingredient_synonyms s
        JOIN drugdb.ingredients i ON i.id = s.id
        WHERE s.synonym IS NOT NULL AND trim(s.synonym) <> ''
    """)
    rows = cur.fetchall()

    syn_exact: dict[str, dict] = {}
    syn_list: list[dict] = []

    for row in rows:
        synonym_lower = row["synonym"].lower()
        entry = {
            "ingredient_id": row["ingredient_id"],
            "drugbank_id": row["drugbank_id"],
            "ingredient_name": row["ingredient_name"],
            "synonym": row["synonym"],
            "synonym_lower": synonym_lower,
        }
        syn_exact[synonym_lower] = entry
        syn_list.append(entry)

    syn_list.sort(key=lambda e: len(e["synonym"]))

    log.info(
        "Loaded %d synonym rows (%d distinct lower-synonym keys)",
        len(syn_list),
        len(syn_exact),
    )
    return syn_exact, syn_list


def load_target_records(cur, log: logging.Logger) -> list[dict]:
    """
    Load all rows from drugdb.indian_brand_ingredient where drugbank_id IS NULL.
    """
    log.info("Loading target records from drugdb.indian_brand_ingredient where drugbank_id IS NULL …")
    cur.execute("""
        SELECT ingredient_name_raw, ingredient_name_norm
        FROM drugdb.indian_brand_ingredient
        WHERE drugbank_id IS NULL
    """)
    rows = cur.fetchall()
    records = [{"ingredient_name_raw": r["ingredient_name_raw"],
                "ingredient_name_norm": r["ingredient_name_norm"]} for r in rows]
    log.info("Found %d records with NULL drugbank_id", len(records))
    return records


# ─────────────────────────────────────────────
# Matching logic
# ─────────────────────────────────────────────

def tier1_exact_ingredient(input_str: str, ing_exact: dict) -> Optional[dict]:
    """Exact case-insensitive match against ingredients.name."""
    return ing_exact.get(input_str.lower())


def tier2_exact_synonym(input_str: str, syn_exact: dict) -> Optional[dict]:
    """Exact case-insensitive match against ingredient_synonyms.synonym."""
    return syn_exact.get(input_str.lower())


def tier3_prefix_ingredient(input_str: str, ing_list: list) -> Optional[dict]:
    """
    Prefix match: ingredients.name.lower().startswith(input_str.lower()).
    ing_list is sorted by name length ascending, so the first hit is the shortest.
    Skips exact matches (already handled by Tier 1).
    """
    prefix = input_str.lower()
    for entry in ing_list:
        name_lower = entry["name_lower"]
        if name_lower.startswith(prefix) and name_lower != prefix:
            return entry
    return None


def tier4_prefix_synonym(input_str: str, syn_list: list) -> Optional[dict]:
    """
    Prefix match: ingredient_synonyms.synonym.lower().startswith(input_str.lower()).
    syn_list is sorted by synonym length ascending, so the first hit is the shortest.
    Skips exact matches (already handled by Tier 2).
    """
    prefix = input_str.lower()
    for entry in syn_list:
        synonym_lower = entry["synonym_lower"]
        if synonym_lower.startswith(prefix) and synonym_lower != prefix:
            return entry
    return None


def match_one(
    candidate: str,
    ing_exact: dict,
    ing_list: list,
    syn_exact: dict,
    syn_list: list,
) -> Optional[tuple[int, str, dict]]:
    """
    Run 4-tier matching against a single candidate string.

    Returns (tier_number, tier_name, match_entry) or None.
    """
    result = tier1_exact_ingredient(candidate, ing_exact)
    if result:
        return (1, "ingredients.name exact", result)

    result = tier2_exact_synonym(candidate, syn_exact)
    if result:
        return (2, "ingredient_synonyms exact", result)

    result = tier3_prefix_ingredient(candidate, ing_list)
    if result:
        return (3, "prefix in ingredients.name", result)

    result = tier4_prefix_synonym(candidate, syn_list)
    if result:
        return (4, "prefix in synonyms", result)

    return None


def match_record(
    record: dict,
    ing_exact: dict,
    ing_list: list,
    syn_exact: dict,
    syn_list: list,
) -> Optional[dict]:
    """
    Try ingredient_name_raw first, then ingredient_name_norm.
    Returns a match_log entry dict, or None if no match found.
    """
    raw = record["ingredient_name_raw"] or ""
    norm = record["ingredient_name_norm"] or ""

    # Try raw first
    if raw:
        hit = match_one(raw, ing_exact, ing_list, syn_exact, syn_list)
        if hit:
            tier, tier_name, entry = hit
            return _build_log_entry(record, raw, tier, tier_name, entry)

    # Fall back to norm
    if norm and norm != raw:
        hit = match_one(norm, ing_exact, ing_list, syn_exact, syn_list)
        if hit:
            tier, tier_name, entry = hit
            return _build_log_entry(record, norm, tier, tier_name, entry)

    return None


def _build_log_entry(record: dict, matched_with: str, tier: int, tier_name: str, entry: dict) -> dict:
    """Build a uniform match_log entry from a match result."""
    # Tier 1 and 3 entries come from ing_exact/ing_list: keys id, drugbank_id, name
    # Tier 2 and 4 entries come from syn_exact/syn_list: keys ingredient_id, drugbank_id, ingredient_name
    ingredient_name = entry.get("name") or entry.get("ingredient_name") or ""
    return {
        "ingredient_name_raw": record["ingredient_name_raw"],
        "ingredient_name_norm": record["ingredient_name_norm"],
        "matched_with": matched_with,
        "tier": tier,
        "tier_name": tier_name,
        "drugbank_id": entry.get("drugbank_id") or "",
        "ingredient_name": ingredient_name,
    }


# ─────────────────────────────────────────────
# Phase 1 — dry-run matching
# ─────────────────────────────────────────────

def phase1_match(
    records: list[dict],
    ing_exact: dict,
    ing_list: list,
    syn_exact: dict,
    syn_list: list,
    log: logging.Logger,
    stats: Stats,
) -> tuple[list[dict], list[dict]]:
    """
    Run matching for all records without writing to DB.

    Returns:
        match_log   — list of match_log entries for matched records
        unmatched   — list of {ingredient_name_raw, ingredient_name_norm} for unmatched records
    """
    match_log: list[dict] = []
    unmatched: list[dict] = []

    total = len(records)
    stats.total_records = total

    for i, record in enumerate(records, 1):
        if i % 100 == 0 or i == total:
            log.info("Processed %d/%d records (%.1f%%)", i, total, i / total * 100)

        log_entry = match_record(record, ing_exact, ing_list, syn_exact, syn_list)

        if log_entry:
            match_log.append(log_entry)
            tier = log_entry["tier"]
            if tier == 1:
                stats.tier1_count += 1
            elif tier == 2:
                stats.tier2_count += 1
            elif tier == 3:
                stats.tier3_count += 1
            elif tier == 4:
                stats.tier4_count += 1
            log.debug(
                "Tier %d match: raw=%r norm=%r → drugbank_id=%s",
                tier,
                record["ingredient_name_raw"],
                record["ingredient_name_norm"],
                log_entry["drugbank_id"],
            )
        else:
            unmatched.append({
                "ingredient_name_raw": record["ingredient_name_raw"],
                "ingredient_name_norm": record["ingredient_name_norm"],
            })
            stats.unmatched_count += 1
            log.debug(
                "No match: raw=%r norm=%r",
                record["ingredient_name_raw"],
                record["ingredient_name_norm"],
            )

    return match_log, unmatched


# ─────────────────────────────────────────────
# Phase 1 — report and statistics JSON
# ─────────────────────────────────────────────

TIER_LABELS = {
    1: "Tier 1 (ingredients.name exact)",
    2: "Tier 2 (ingredient_synonyms exact)",
    3: "Tier 3 (prefix in ingredients.name)",
    4: "Tier 4 (prefix in synonyms)",
}


def print_phase1_report(stats: Stats, match_log: list[dict], unmatched: list[dict]):
    """Print the Phase 1 dry-run results to stdout (interactive output)."""
    print()
    print("=== DRY RUN RESULTS ===")
    print(f"Total records to process: {stats.total_records}")
    print(f"Matched via Tier 1 (ingredients.name exact):      {stats.tier1_count} records")
    print(f"Matched via Tier 2 (ingredient_synonyms exact):   {stats.tier2_count} records")
    print(f"Matched via Tier 3 (prefix in ingredients.name):  {stats.tier3_count} records")
    print(f"Matched via Tier 4 (prefix in synonyms):          {stats.tier4_count} records")
    print(f"Total matched:   {stats.matched_count} records")
    print(f"Total unmatched: {stats.unmatched_count} records")
    print(f"Match rate: {stats.match_rate_pct}%")
    print()

    # Sample matches per tier
    for tier_num in (1, 2, 3, 4):
        tier_entries = [e for e in match_log if e["tier"] == tier_num]
        samples = tier_entries[:5]
        if not samples:
            continue
        label = TIER_LABELS[tier_num]
        print(f"--- {label} samples ({min(5, len(tier_entries))} of {len(tier_entries)}) ---")
        for e in samples:
            print(
                f"  raw={e['ingredient_name_raw']!r}  norm={e['ingredient_name_norm']!r}"
                f"  matched_with={e['matched_with']!r}"
                f"  drugbank_id={e['drugbank_id']!r}"
                f"  ingredient={e['ingredient_name']!r}"
            )
        print()

    # Unmatched records
    if unmatched:
        print(f"--- Unmatched records ({len(unmatched)}) ---")
        for u in unmatched:
            print(f"  raw={u['ingredient_name_raw']!r}  norm={u['ingredient_name_norm']!r}")
        print()


def save_statistics_json(stats: Stats, match_log: list[dict], unmatched: list[dict]):
    """Write match_statistics.json to the current working directory."""

    def _examples(tier_num: int) -> list[dict]:
        entries = [e for e in match_log if e["tier"] == tier_num][:5]
        return [
            {
                "ingredient_name_raw": e["ingredient_name_raw"],
                "ingredient_name_norm": e["ingredient_name_norm"],
                "matched_with": e["matched_with"],
                "drugbank_id": e["drugbank_id"],
                "ingredient_name": e["ingredient_name"],
            }
            for e in entries
        ]

    payload = {
        "generated_at": datetime.now().isoformat(),
        "total_records": stats.total_records,
        "matched": stats.matched_count,
        "unmatched": stats.unmatched_count,
        "match_rate_pct": stats.match_rate_pct,
        "tier_1_count": stats.tier1_count,
        "tier_2_count": stats.tier2_count,
        "tier_3_count": stats.tier3_count,
        "tier_4_count": stats.tier4_count,
        "tier_1_examples": _examples(1),
        "tier_2_examples": _examples(2),
        "tier_3_examples": _examples(3),
        "tier_4_examples": _examples(4),
        "unmatched_list": unmatched,
    }

    output_path = "match_statistics.json"
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)

    print(f"Statistics saved to {output_path}")


# ─────────────────────────────────────────────
# Phase 2 — apply updates
# ─────────────────────────────────────────────

UPDATE_SQL = """
    UPDATE drugdb.indian_brand_ingredient
    SET drugbank_id = %s
    WHERE ingredient_name_raw = %s AND drugbank_id IS NULL
"""


def phase2_update(
    conn,
    match_log: list[dict],
    batch_size: int,
    log: logging.Logger,
    stats: Stats,
):
    """Apply updates from match_log in batches of batch_size."""
    total = len(match_log)
    log.info("Phase 2: applying %d updates (batch_size=%d)", total, batch_size)

    tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    batch_cur = conn.cursor()
    batch_count = 0

    for i, entry in enumerate(match_log, 1):
        if i % 100 == 0 or i == total:
            log.info("Processed %d/%d records (%.1f%%)", i, total, i / total * 100)

        if not entry.get("drugbank_id"):
            log.warning(
                "Skipping row with empty drugbank_id: raw=%r",
                entry["ingredient_name_raw"],
            )
            continue

        try:
            batch_cur.execute(UPDATE_SQL, (entry["drugbank_id"], entry["ingredient_name_raw"]))
            tier_counts[entry["tier"]] += 1
            batch_count += 1

            if batch_count >= batch_size:
                conn.commit()
                log.info("Committed batch of %d", batch_count)
                batch_count = 0

        except Exception as exc:
            msg = (
                f"Error updating raw={entry['ingredient_name_raw']!r} "
                f"drugbank_id={entry['drugbank_id']!r}: {exc}"
            )
            log.error(msg)
            stats.update_errors += 1
            stats.error_details.append(msg)
            try:
                conn.rollback()
            except Exception:
                pass
            batch_count = 0

    if batch_count > 0:
        conn.commit()
        log.info("Committed final batch of %d", batch_count)

    batch_cur.close()

    print()
    print("=== PHASE 2 UPDATE RESULTS ===")
    print(f"Updated via Tier 1 (ingredients.name exact):      {tier_counts[1]} records")
    print(f"Updated via Tier 2 (ingredient_synonyms exact):   {tier_counts[2]} records")
    print(f"Updated via Tier 3 (prefix in ingredients.name):  {tier_counts[3]} records")
    print(f"Updated via Tier 4 (prefix in synonyms):          {tier_counts[4]} records")
    print(f"Total updated: {sum(tier_counts.values())} records")
    print(f"Errors: {stats.update_errors}")
    if stats.error_details:
        print("Error details:")
        for detail in stats.error_details[:20]:
            print(f"  {detail}")
        if len(stats.error_details) > 20:
            print(f"  … and {len(stats.error_details) - 20} more (see log file)")
    print()


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Update drugdb.indian_brand_ingredient.drugbank_id (NULL rows) "
            "by matching ingredient names against drugdb.ingredients and "
            "drugdb.ingredient_synonyms via 4-tier logic."
        )
    )
    p.add_argument("--host",        default=os.environ.get("DB_HOST", "localhost"),
                   help="PostgreSQL host (default: localhost)")
    p.add_argument("--dbname",      default="postgres",
                   help="Database name (default: postgres)")
    p.add_argument("--user",        default="postgres",
                   help="Database user (default: postgres)")
    p.add_argument("--password",    required=True,
                   help="PostgreSQL password (required)")
    p.add_argument("--port",        type=int, default=5432,
                   help="PostgreSQL port (default: 5432)")
    p.add_argument("--batch-size",  type=int, default=500,
                   help="Commit every N updates in Phase 2 (default: 500)")
    p.add_argument("--log-file",    help="Write detailed log to this file path")
    p.add_argument("--verbose",     action="store_true",
                   help="Print DEBUG-level messages to console")
    p.add_argument("--skip-confirm", action="store_true",
                   help="Auto-proceed to Phase 2 without interactive prompt")
    return p


def setup_logging(verbose: bool, log_file: Optional[str]) -> logging.Logger:
    log = logging.getLogger("indian_brand_drugbank_update")
    log.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    log.addHandler(console)

    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        log.addHandler(fh)

    return log


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    args = build_parser().parse_args()
    log = setup_logging(args.verbose, args.log_file)

    log.info(
        "Connecting to %s:%d/%s as %s",
        args.host, args.port, args.dbname, args.user,
    )
    try:
        conn = get_connection(args.host, args.dbname, args.user, args.password, args.port)
    except Exception as exc:
        log.error("Connection failed: %s", exc)
        sys.exit(1)

    stats = Stats()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # ── Pre-load reference data ──────────────────────────────────────
            ing_exact, ing_list = load_ingredients(cur, log)
            syn_exact, syn_list = load_synonyms(cur, log)

            # ── Load target rows ─────────────────────────────────────────────
            records = load_target_records(cur, log)

    except Exception as exc:
        log.error("Failed to load data from database: %s", exc, exc_info=True)
        conn.close()
        sys.exit(1)

    if not records:
        log.info("No rows with NULL drugbank_id found — nothing to do.")
        conn.close()
        return

    # ── Phase 1: match without writing ──────────────────────────────────────
    log.info("=== Phase 1: running matching (no DB writes) ===")
    match_log, unmatched = phase1_match(
        records, ing_exact, ing_list, syn_exact, syn_list, log, stats
    )

    print_phase1_report(stats, match_log, unmatched)
    save_statistics_json(stats, match_log, unmatched)

    if not match_log:
        log.info("No matches found — nothing to update.")
        conn.close()
        return

    # ── Prompt for Phase 2 ───────────────────────────────────────────────────
    if args.skip_confirm:
        proceed = True
        log.info("--skip-confirm set: proceeding to Phase 2 automatically.")
    else:
        try:
            answer = input("Do you want to proceed with the actual UPDATE? (yes/no): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = "no"
            print()

        proceed = answer == "yes"

    if not proceed:
        log.info("User declined — exiting without writing any updates.")
        conn.close()
        return

    # ── Phase 2: apply updates ───────────────────────────────────────────────
    log.info("=== Phase 2: applying updates ===")
    try:
        phase2_update(conn, match_log, args.batch_size, log, stats)
    except KeyboardInterrupt:
        log.warning("Interrupted by user — rolling back open transaction")
        try:
            conn.rollback()
        except Exception:
            pass
    except Exception as exc:
        log.error("Unexpected error during Phase 2: %s", exc, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()

    log.info("Done.")


if __name__ == "__main__":
    main()
