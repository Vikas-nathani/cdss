"""
fill_null_siblings.py

Fills NULL values in drugdb.drug for formulations present in
drugdb.drug_formulation_linkage_map_unique by borrowing values from sibling
formulations that share the same generic_formulation + dosage_forms.

Fields filled:
  - routes               (array)
  - mechanism_of_action  (text)
  - pharmacologic_class  (array)
  - therapeutic_class    (array)
  - mechanism_class      (array)

Conflict resolution:
  - text fields  → pick the LONGEST non-null value across siblings
  - array fields → UNION all non-null sibling values + deduplicate

Usage:
  python fill_null_siblings.py --dry-run
  python fill_null_siblings.py --full-run
"""

# ─────────────────────────────────────────────
# 1. IMPORTS + CONFIG + LOGGING SETUP
# ─────────────────────────────────────────────
import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).parent
LOGS_DIR = SCRIPT_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILENAME = LOGS_DIR / f"sibling_fill_{RUN_TIMESTAMP}.log"

TEXT_FIELDS = ["mechanism_of_action"]
ARRAY_FIELDS = ["routes", "pharmacologic_class", "therapeutic_class", "mechanism_class"]
ALL_FIELDS = TEXT_FIELDS + ARRAY_FIELDS


def setup_logging():
    logger = logging.getLogger("sibling_fill")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = logging.FileHandler(LOG_FILENAME, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


log = setup_logging()


# ─────────────────────────────────────────────
# 2. DB CONNECTION
# ─────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


# ─────────────────────────────────────────────
# 3. PRE/POST NULL AUDIT
# ─────────────────────────────────────────────
NULL_AUDIT_SQL = """
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE routes         IS NULL OR array_length(routes, 1) IS NULL)                             AS null_routes,
    COUNT(*) FILTER (WHERE mechanism_of_action IS NULL OR mechanism_of_action = '')                               AS null_moa,
    COUNT(*) FILTER (WHERE pharmacologic_class IS NULL OR array_length(pharmacologic_class, 1) IS NULL)           AS null_pharmacologic,
    COUNT(*) FILTER (WHERE therapeutic_class   IS NULL OR array_length(therapeutic_class,   1) IS NULL)           AS null_therapeutic,
    COUNT(*) FILTER (WHERE mechanism_class     IS NULL OR array_length(mechanism_class,     1) IS NULL)           AS null_mechanism
FROM drugdb.drug d
WHERE EXISTS (
    SELECT 1 FROM drugdb.drug_formulation_linkage_map_unique m
    WHERE m.formulation_id = d.formulation_id
);
"""


def run_null_audit(conn) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(NULL_AUDIT_SQL)
        row = cur.fetchone()
    return {
        "total":              row["total"],
        "routes":             row["null_routes"],
        "mechanism_of_action": row["null_moa"],
        "pharmacologic_class": row["null_pharmacologic"],
        "therapeutic_class":   row["null_therapeutic"],
        "mechanism_class":     row["null_mechanism"],
    }


def log_audit(label: str, audit: dict):
    log.info(f"── NULL AUDIT ({label}) ──────────────────────────")
    log.info(f"  Total formulations in scope : {audit['total']}")
    for field in ALL_FIELDS:
        null_count = audit[field]
        pct = (null_count / audit["total"] * 100) if audit["total"] else 0
        log.info(f"  {field:<26}: {null_count:>5} null  ({pct:.1f}%)")
    log.info("─" * 52)


# ─────────────────────────────────────────────
# 4. SIBLING FINDER QUERY
# ─────────────────────────────────────────────
SIBLING_QUERY = """
SELECT
    target.formulation_id        AS null_formulation_id,
    target.generic_formulation,
    target.dosage_forms,
    -- target's current values (to check which fields are actually null)
    target.mechanism_of_action   AS target_mechanism_of_action,
    target.pharmacologic_class   AS target_pharmacologic_class,
    target.therapeutic_class     AS target_therapeutic_class,
    target.mechanism_class       AS target_mechanism_class,
    target.routes                AS target_routes,
    -- sibling donor values
    sibling.formulation_id       AS sibling_formulation_id,
    sibling.mechanism_of_action,
    sibling.pharmacologic_class,
    sibling.therapeutic_class,
    sibling.mechanism_class,
    sibling.routes
FROM drugdb.drug target
JOIN drugdb.drug_formulation_linkage_map_unique m
    ON m.formulation_id = target.formulation_id
JOIN drugdb.drug sibling
    ON  sibling.generic_formulation = target.generic_formulation
    AND sibling.dosage_forms        = target.dosage_forms
    AND sibling.formulation_id     != target.formulation_id
WHERE (
    target.mechanism_of_action IS NULL OR target.mechanism_of_action = ''
    OR target.pharmacologic_class IS NULL OR target.pharmacologic_class = '{}'
    OR target.therapeutic_class   IS NULL OR target.therapeutic_class   = '{}'
    OR target.mechanism_class     IS NULL OR target.mechanism_class     = '{}'
    OR target.routes              IS NULL OR target.routes              = '{}'
)
AND (
    (sibling.mechanism_of_action IS NOT NULL AND sibling.mechanism_of_action != '')
    OR (sibling.pharmacologic_class IS NOT NULL AND sibling.pharmacologic_class != '{}')
    OR (sibling.therapeutic_class   IS NOT NULL AND sibling.therapeutic_class   != '{}')
    OR (sibling.mechanism_class     IS NOT NULL AND sibling.mechanism_class     != '{}')
    OR (sibling.routes              IS NOT NULL AND sibling.routes              != '{}')
)
ORDER BY target.formulation_id, sibling.formulation_id;
"""


def _is_null_field(value, field_name: str) -> bool:
    if value is None:
        return True
    if field_name in TEXT_FIELDS and str(value).strip() == "":
        return True
    if field_name in ARRAY_FIELDS and (isinstance(value, list) and len(value) == 0):
        return True
    return False


def fetch_sibling_pairs(conn) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SIBLING_QUERY)
        return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────
# 5. CONFLICT RESOLVER
# ─────────────────────────────────────────────
def resolve_text(values: list[str]) -> str:
    non_null = [v for v in values if v and str(v).strip()]
    if not non_null:
        return None
    return max(non_null, key=len)


def resolve_array(values: list) -> list:
    seen = set()
    result = []
    for arr in values:
        if not arr:
            continue
        for item in arr:
            key = str(item).strip().lower()
            if key and key not in seen:
                seen.add(key)
                result.append(item)
    return result if result else None


def resolve_field(field: str, sibling_values: list):
    if field in TEXT_FIELDS:
        return resolve_text(sibling_values)
    else:
        return resolve_array(sibling_values)


# ─────────────────────────────────────────────
# 6 + 7. DRY RUN / FULL RUN LOGIC (shared core)
# ─────────────────────────────────────────────
def compute_fills(sibling_pairs: list[dict]) -> list[dict]:
    """
    Process (target, sibling) pairs returned by SIBLING_QUERY.
    Groups by null_formulation_id, collects per-field donor values from
    siblings, applies conflict resolution, returns fill-action dicts.
    """
    targets: dict = {}
    for row in sibling_pairs:
        fid = row["null_formulation_id"]
        if fid not in targets:
            targets[fid] = {
                "formulation_id":      fid,
                "generic_formulation": row["generic_formulation"],
                "dosage_forms":        row["dosage_forms"],
                # target's own current values — used to skip already-filled fields
                "target_values": {
                    f: row[f"target_{f}"] for f in ALL_FIELDS
                },
                "field_donors": {f: [] for f in ALL_FIELDS},
            }
        for field in ALL_FIELDS:
            v = row[field]  # sibling's value
            if not _is_null_field(v, field):
                targets[fid]["field_donors"][field].append(
                    (row["sibling_formulation_id"], v)
                )

    actions = []
    for fid, target in targets.items():
        for field in ALL_FIELDS:
            # skip if target already has a value for this field
            if not _is_null_field(target["target_values"][field], field):
                continue

            donors = target["field_donors"][field]
            if not donors:
                continue

            sibling_ids = [d[0] for d in donors]
            values      = [d[1] for d in donors]

            resolution = "single_sibling" if len(donors) == 1 else (
                "longest" if field in TEXT_FIELDS else "union_dedup"
            )

            filled_value = resolve_field(field, values)
            if filled_value is None:
                continue

            actions.append({
                "formulation_id":      fid,
                "generic_formulation": target["generic_formulation"],
                "dosage_forms":        target["dosage_forms"],
                "field":               field,
                "sibling_ids":         sibling_ids,
                "resolution":          resolution,
                "filled_value":        filled_value,
            })

    return actions


def log_action(action: dict, mode: str):
    fid = action["formulation_id"]
    field = action["field"]
    sibs = action["sibling_ids"]
    resolution = action["resolution"]
    value = action["filled_value"]

    log.debug(f"[{mode}] formulation_id={fid}  field={field}")
    log.debug(f"        generic_formulation={action['generic_formulation']!r}  dosage_forms={action['dosage_forms']!r}")
    log.debug(f"        sibling_ids={sibs}")
    if len(sibs) > 1:
        log.debug(f"        MULTIPLE SIBLINGS FOUND: {sibs}  →  resolution={resolution}")
    log.debug(f"        filled_value={value!r}")


UPDATE_TEXT_SQL = """
UPDATE drugdb.drug
SET {field} = %(value)s
WHERE formulation_id = %(fid)s
  AND ({field} IS NULL OR {field} = '')
"""

UPDATE_ARRAY_SQL = """
UPDATE drugdb.drug
SET {field} = %(value)s
WHERE formulation_id = %(fid)s
  AND ({field} IS NULL OR array_length({field}, 1) IS NULL)
"""


def execute_fill(conn, action: dict):
    field = action["field"]
    fid = action["formulation_id"]
    value = action["filled_value"]

    if field in TEXT_FIELDS:
        sql = UPDATE_TEXT_SQL.format(field=field)
        params = {"value": value, "fid": fid}
    else:
        sql = UPDATE_ARRAY_SQL.format(field=field)
        params = {"value": value, "fid": fid}

    with conn.cursor() as cur:
        cur.execute(sql, params)


def run_dry(conn):
    log.info("=" * 60)
    log.info("MODE: DRY RUN  (no writes to DB)")
    log.info("=" * 60)

    pre_audit = run_null_audit(conn)
    log_audit("PRE", pre_audit)

    rows = fetch_sibling_pairs(conn)
    log.info(f"Fetched {len(rows)} sibling pairs from DB")

    actions = compute_fills(rows)
    log.info(f"Total fill actions projected: {len(actions)}")

    # per-field fill counts
    fill_counts: dict[str, int] = {f: 0 for f in ALL_FIELDS}
    for action in actions:
        log_action(action, "DRY-RUN")
        fill_counts[action["field"]] += 1

    _print_summary(pre_audit, fill_counts, projected=True)


def run_full(conn):
    log.info("=" * 60)
    log.info("MODE: FULL RUN  (writing to DB)")
    log.info("=" * 60)

    pre_audit = run_null_audit(conn)
    log_audit("PRE", pre_audit)

    rows = fetch_sibling_pairs(conn)
    log.info(f"Fetched {len(rows)} sibling pairs from DB")

    actions = compute_fills(rows)
    log.info(f"Total fill actions to execute: {len(actions)}")

    fill_counts: dict[str, int] = {f: 0 for f in ALL_FIELDS}
    for action in actions:
        log_action(action, "FULL-RUN")
        execute_fill(conn, action)
        fill_counts[action["field"]] += 1

    conn.commit()
    log.info("DB commit successful")

    post_audit = run_null_audit(conn)
    log_audit("POST", post_audit)

    _print_summary(pre_audit, fill_counts, projected=False, post_audit=post_audit)


# ─────────────────────────────────────────────
# 9. SUMMARY REPORT
# ─────────────────────────────────────────────
def _print_summary(pre: dict, fill_counts: dict, projected: bool, post_audit: dict = None):
    label = "PROJECTED" if projected else "ACTUAL"
    log.info("")
    log.info("=" * 60)
    log.info(f"SUMMARY REPORT  [{label}]")
    log.info("=" * 60)
    log.info(f"  {'Field':<26}  {'Null Before':>11}  {'Filled':>8}  {'Null After':>10}  {'Still Null':>10}")
    log.info("  " + "-" * 72)

    for field in ALL_FIELDS:
        null_before = pre[field]
        filled = fill_counts.get(field, 0)

        if post_audit:
            null_after = post_audit[field]
        else:
            null_after = max(0, null_before - filled)

        still_null = null_after
        log.info(f"  {field:<26}  {null_before:>11}  {filled:>8}  {null_after:>10}  {still_null:>10}")

    log.info("=" * 60)
    log.info(f"Log file written to: {LOG_FILENAME}")


# ─────────────────────────────────────────────
# 10. MAIN + ARGPARSE
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Fill NULL fields in drugdb.drug via sibling formulation borrowing."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and log fills without writing to DB",
    )
    group.add_argument(
        "--full-run",
        action="store_true",
        help="Compute fills and write them to DB",
    )
    args = parser.parse_args()

    log.info(f"sibling_fill starting  |  log={LOG_FILENAME}")

    conn = None
    try:
        conn = get_connection()
        log.info("DB connection established")

        if args.dry_run:
            run_dry(conn)
        else:
            run_full(conn)

    except Exception:
        log.exception("Fatal error during sibling_fill run")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            log.debug("DB connection closed")


if __name__ == "__main__":
    main()
