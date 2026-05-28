#!/usr/bin/env python3
"""
Update drugdb.ingredients.rxcui from DrugMasterLinkage.combined_clean_jsonb

Both source (public.DrugMasterLinkage) and target (drugdb.ingredients,
drugdb.ingredient_synonyms) live in the same 'postgres' database, so a single
connection is used throughout.

Only processes ingredients where drugdb.ingredients.rxcui IS NULL.
"""

import argparse
import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────

@dataclass
class Ingredient:
    name: str
    ing_rxcui: str

    def __post_init__(self):
        self.name = self.name.strip()
        self.ing_rxcui = self.ing_rxcui.strip()


@dataclass
class Stats:
    total_extracted: int = 0
    skipped_null: int = 0
    method1_exact_name: int = 0
    method2_prefix_name: int = 0
    method3_exact_synonym: int = 0
    method4_prefix_synonym: int = 0
    updated_null_to_value: int = 0
    updated_changed_value: int = 0
    skipped_already_correct: int = 0
    inserted: int = 0
    errors: int = 0
    error_details: list = field(default_factory=list)

    @property
    def total_processed(self):
        return (
            self.updated_null_to_value
            + self.updated_changed_value
            + self.skipped_already_correct
            + self.inserted
        )

    @property
    def total_updates(self):
        return self.updated_null_to_value + self.updated_changed_value


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


def extract_ingredients_from_source(cur) -> list[Ingredient]:
    """
    Pull every ingredient from DrugMasterLinkage.combined_clean_jsonb->rxnorm[*]->ingredients[*].
    Deduplicates by (name_lower, ing_rxcui) across all rows.
    """
    sql = """
        SELECT DISTINCT
            lower(trim(ing->>'name'))    AS name_lower,
            trim(ing->>'name')           AS name,
            trim(ing->>'ing_rxcui')      AS ing_rxcui
        FROM
            public."DrugMasterLinkage",
            jsonb_array_elements(
                CASE
                    WHEN combined_clean_jsonb IS NOT NULL
                         AND jsonb_typeof(combined_clean_jsonb->'rxnorm') = 'array'
                    THEN combined_clean_jsonb->'rxnorm'
                    ELSE '[]'::jsonb
                END
            ) AS rxn,
            jsonb_array_elements(
                CASE
                    WHEN jsonb_typeof(rxn->'ingredients') = 'array'
                    THEN rxn->'ingredients'
                    ELSE '[]'::jsonb
                END
            ) AS ing
        WHERE
            (ing->>'name') IS NOT NULL
            AND trim(ing->>'name') <> ''
            AND (ing->>'ing_rxcui') IS NOT NULL
            AND trim(ing->>'ing_rxcui') <> ''
    """
    cur.execute(sql)
    rows = cur.fetchall()
    seen = set()
    results = []
    for row in rows:
        key = (row["name_lower"], row["ing_rxcui"])
        if key not in seen:
            seen.add(key)
            results.append(Ingredient(name=row["name"], ing_rxcui=row["ing_rxcui"]))
    return results


def load_all_ingredients(cur) -> dict:
    """
    Load ALL ingredients regardless of rxcui status.
    Returns dict keyed by lower(trim(name)) -> list of {id, name, rxcui}.
    """
    cur.execute("""
        SELECT id::text, trim(name) AS name, rxcui
        FROM drugdb.ingredients
    """)
    index: dict[str, list] = {}
    for row in cur.fetchall():
        key = row["name"].lower()
        index.setdefault(key, []).append({
            "id": row["id"],
            "name": row["name"],
            "rxcui": row["rxcui"],
        })
    return index


def load_all_synonyms(cur) -> dict:
    """
    Load ALL synonyms regardless of ingredient rxcui status.
    Returns dict keyed by lower(trim(synonym)) -> list of {id, rxcui}.
    """
    cur.execute("""
        SELECT s.id::text, trim(s.synonym) AS synonym, i.rxcui
        FROM drugdb.ingredient_synonyms s
        JOIN drugdb.ingredients i ON i.id = s.id
        WHERE s.synonym IS NOT NULL
          AND trim(s.synonym) <> ''
    """)
    index: dict[str, list] = {}
    for row in cur.fetchall():
        key = row["synonym"].lower()
        index.setdefault(key, []).append({
            "id": row["id"],
            "rxcui": row["rxcui"],
        })
    return index


# ─────────────────────────────────────────────
# Matching methods
# ─────────────────────────────────────────────

def method1_exact_name(ing: Ingredient, name_index: dict) -> tuple[Optional[str], Optional[str]]:
    """Return (ingredient_id, current_rxcui) where name matches exactly (case-insensitive)."""
    key = ing.name.lower()
    matches = name_index.get(key)
    if matches:
        entry = matches[0]
        return (entry["id"], entry["rxcui"])
    return (None, None)


def method2_prefix_name(ing: Ingredient, name_index: dict) -> tuple[Optional[str], Optional[str]]:
    """Return (ingredient_id, current_rxcui) where DB name starts with the source name."""
    prefix = ing.name.lower()
    for key, entries in name_index.items():
        if key.startswith(prefix) and key != prefix:
            entry = entries[0]
            return (entry["id"], entry["rxcui"])
    return (None, None)


def method3_exact_synonym(ing: Ingredient, synonym_index: dict) -> tuple[Optional[str], Optional[str]]:
    """Return (ingredient_id, current_rxcui) where a synonym matches exactly (case-insensitive)."""
    key = ing.name.lower()
    matches = synonym_index.get(key)
    if matches:
        entry = matches[0]
        return (entry["id"], entry["rxcui"])
    return (None, None)


def method4_prefix_synonym(ing: Ingredient, synonym_index: dict) -> tuple[Optional[str], Optional[str]]:
    """Return (ingredient_id, current_rxcui) where a synonym starts with the source name."""
    prefix = ing.name.lower()
    for key, entries in synonym_index.items():
        if key.startswith(prefix) and key != prefix:
            entry = entries[0]
            return (entry["id"], entry["rxcui"])
    return (None, None)


# ─────────────────────────────────────────────
# Database writes
# ─────────────────────────────────────────────

def do_update(cur, ingredient_id: str, rxcui: str, dry_run: bool, log: logging.Logger):
    if dry_run:
        log.debug("DRY-RUN UPDATE id=%s rxcui=%s", ingredient_id, rxcui)
        return
    cur.execute(
        "UPDATE drugdb.ingredients SET rxcui = %s WHERE id = %s::uuid",
        (rxcui, ingredient_id),
    )


def do_insert(cur, name: str, rxcui: str, dry_run: bool, log: logging.Logger):
    new_id = str(uuid.uuid4())
    if dry_run:
        log.debug("DRY-RUN INSERT name=%r rxcui=%s", name, rxcui)
        return
    cur.execute(
        """
        INSERT INTO drugdb.ingredients
            (id, name, rxcui, drugbank_id, unii, indications,
             general_function, pharmacodynamics, classification_description, food_interactions)
        VALUES (%s::uuid, %s, %s, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        ON CONFLICT DO NOTHING
        """,
        (new_id, name, rxcui),
    )


# ─────────────────────────────────────────────
# Core processing loop
# ─────────────────────────────────────────────

def process_ingredients(
    conn,
    ingredients: list[Ingredient],
    dry_run: bool,
    batch_size: int,
    log: logging.Logger,
    stats: Stats,
):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        log.info("Loading ALL ingredients and synonyms into memory …")
        name_index = load_all_ingredients(cur)
        synonym_index = load_all_synonyms(cur)
        log.info(
            "Loaded %d distinct ingredient names, %d synonym keys",
            len(name_index),
            len(synonym_index),
        )

    total = len(ingredients)
    batch_cur = conn.cursor()
    batch_count = 0

    for i, ing in enumerate(ingredients, 1):
        if i % 500 == 0 or i == total:
            log.info("Progress: %d / %d", i, total)

        try:
            matched_id = None
            current_rxcui = None
            method_used = None

            matched_id, current_rxcui = method1_exact_name(ing, name_index)
            if matched_id:
                method_used = 1
                stats.method1_exact_name += 1

            if not matched_id:
                matched_id, current_rxcui = method2_prefix_name(ing, name_index)
                if matched_id:
                    method_used = 2
                    stats.method2_prefix_name += 1

            if not matched_id:
                matched_id, current_rxcui = method3_exact_synonym(ing, synonym_index)
                if matched_id:
                    method_used = 3
                    stats.method3_exact_synonym += 1

            if not matched_id:
                matched_id, current_rxcui = method4_prefix_synonym(ing, synonym_index)
                if matched_id:
                    method_used = 4
                    stats.method4_prefix_synonym += 1

            if matched_id:
                if current_rxcui is None:
                    log.debug("Method %d (NULL→%s): %r", method_used, ing.ing_rxcui, ing.name)
                    do_update(batch_cur, matched_id, ing.ing_rxcui, dry_run, log)
                    stats.updated_null_to_value += 1
                    _remove_from_name_index(name_index, matched_id)
                    _remove_from_synonym_index(synonym_index, matched_id)
                elif current_rxcui != ing.ing_rxcui:
                    log.debug("Method %d (CHANGED %s→%s): %r",
                              method_used, current_rxcui, ing.ing_rxcui, ing.name)
                    do_update(batch_cur, matched_id, ing.ing_rxcui, dry_run, log)
                    stats.updated_changed_value += 1
                    _remove_from_name_index(name_index, matched_id)
                    _remove_from_synonym_index(synonym_index, matched_id)
                else:
                    log.debug("Method %d (ALREADY CORRECT rxcui=%s): %r",
                              method_used, ing.ing_rxcui, ing.name)
                    stats.skipped_already_correct += 1
            else:
                log.debug("No match for %r — inserting new record rxcui=%s", ing.name, ing.ing_rxcui)
                do_insert(batch_cur, ing.name, ing.ing_rxcui, dry_run, log)
                stats.inserted += 1

            batch_count += 1
            if batch_count >= batch_size:
                if not dry_run:
                    conn.commit()
                    log.info("Committed batch of %d", batch_count)
                batch_count = 0

        except Exception as exc:
            msg = f"Error processing ingredient {ing.name!r} (rxcui={ing.ing_rxcui}): {exc}"
            log.error(msg)
            stats.errors += 1
            stats.error_details.append(msg)
            if not dry_run:
                conn.rollback()
            batch_count = 0

    if batch_count > 0 and not dry_run:
        conn.commit()
        log.info("Committed final batch of %d", batch_count)

    batch_cur.close()


def _remove_from_name_index(index: dict, ingredient_id: str):
    for key in list(index.keys()):
        index[key] = [e for e in index[key] if e["id"] != ingredient_id]
        if not index[key]:
            del index[key]


def _remove_from_synonym_index(index: dict, ingredient_id: str):
    for key in list(index.keys()):
        index[key] = [i for i in index[key] if i != ingredient_id]
        if not index[key]:
            del index[key]


# ─────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────

REPORT_TEMPLATE = """
============================================
RXCUI Update Summary  ({timestamp})
============================================
Total ingredients extracted:   {total_extracted:>10,}
Skipped (null name/rxcui):     {skipped_null:>10,}

Matched by Method:
  Method 1 (Exact Name):       {method1:>10,}
  Method 2 (Prefix Name):      {method2:>10,}
  Method 3 (Exact Synonym):    {method3:>10,}
  Method 4 (Prefix Synonym):   {method4:>10,}

Actions Taken:
  Updated (NULL → rxcui):      {updated_null:>10,}
  Updated (rxcui changed):     {updated_changed:>10,}
  Skipped (already correct):   {skipped_correct:>10,}
  Inserted (new ingredients):  {inserted:>10,}

Total Updates:                 {total_updates:>10,}
Total Processed:               {total_processed:>10,}
Errors:                        {errors:>10,}
============================================

Verification Queries (run against 'postgres' database):

-- Records still missing rxcui
SELECT COUNT(*) FROM drugdb.ingredients WHERE rxcui IS NULL;

-- Total with rxcui populated
SELECT COUNT(*) FROM drugdb.ingredients WHERE rxcui IS NOT NULL;

-- Total ingredient count
SELECT COUNT(*) FROM drugdb.ingredients;

-- Newly inserted records (drugbank_id IS NULL, rxcui NOT NULL)
SELECT id, name, rxcui
FROM drugdb.ingredients
WHERE drugbank_id IS NULL AND rxcui IS NOT NULL
LIMIT 20;
"""


def print_report(stats: Stats):
    print(REPORT_TEMPLATE.format(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total_extracted=stats.total_extracted,
        skipped_null=stats.skipped_null,
        method1=stats.method1_exact_name,
        method2=stats.method2_prefix_name,
        method3=stats.method3_exact_synonym,
        method4=stats.method4_prefix_synonym,
        updated_null=stats.updated_null_to_value,
        updated_changed=stats.updated_changed_value,
        skipped_correct=stats.skipped_already_correct,
        inserted=stats.inserted,
        total_updates=stats.total_updates,
        total_processed=stats.total_processed,
        errors=stats.errors,
    ))
    if stats.error_details:
        print("Error details:")
        for detail in stats.error_details[:20]:
            print(f"  {detail}")
        if len(stats.error_details) > 20:
            print(f"  … and {len(stats.error_details) - 20} more (see log file)")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def build_parser():
    p = argparse.ArgumentParser(
        description="Update drugdb.ingredients.rxcui from DrugMasterLinkage (single 'postgres' DB)"
    )
    p.add_argument("--host",       default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--dbname",     default="postgres",
                   help="Database containing both DrugMasterLinkage and drugdb schema (default: postgres)")
    p.add_argument("--user",       default="postgres",
                   help="Database user with access to both public and drugdb schemas (default: postgres)")
    p.add_argument("--password",   required=True, help="PostgreSQL password")
    p.add_argument("--port",       type=int, default=5432)
    p.add_argument("--dry-run",    action="store_true",
                   help="Show what would be changed without writing to the database")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Commit every N records (default: 1000)")
    p.add_argument("--log-file",   help="Write detailed log to this file")
    p.add_argument("--verbose",    action="store_true",
                   help="Print DEBUG-level messages to console")
    return p


def setup_logging(verbose: bool, log_file: Optional[str]) -> logging.Logger:
    log = logging.getLogger("rxcui_update")
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


def main():
    args = build_parser().parse_args()
    log = setup_logging(args.verbose, args.log_file)

    if args.dry_run:
        log.info("*** DRY-RUN MODE — no changes will be written ***")

    log.info("Connecting to %s:%s/%s as %s", args.host, args.port, args.dbname, args.user)
    try:
        conn = get_connection(args.host, args.dbname, args.user, args.password, args.port)
    except Exception as exc:
        log.error("Connection failed: %s", exc)
        sys.exit(1)

    stats = Stats()

    try:
        log.info("Extracting ingredients from DrugMasterLinkage …")
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            all_ingredients = extract_ingredients_from_source(cur)

        stats.total_extracted = len(all_ingredients)
        log.info("Extracted %d unique ingredients", stats.total_extracted)

        if not all_ingredients:
            log.warning("No ingredients found in DrugMasterLinkage — nothing to do.")
            return

        valid = [i for i in all_ingredients if i.name and i.ing_rxcui]
        stats.skipped_null = stats.total_extracted - len(valid)
        if stats.skipped_null:
            log.info("Skipped %d entries with null/empty name or rxcui", stats.skipped_null)

        process_ingredients(conn, valid, args.dry_run, args.batch_size, log, stats)

    except KeyboardInterrupt:
        log.warning("Interrupted by user — rolling back open transaction")
        conn.rollback()
    except Exception as exc:
        log.error("Unexpected error: %s", exc, exc_info=True)
        conn.rollback()
    finally:
        conn.close()

    print_report(stats)


if __name__ == "__main__":
    main()
