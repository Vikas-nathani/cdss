#!/usr/bin/env python3
"""
Creates drugdb.masterlinkage_unique as a physical table with one row per unique
generic_name, selecting the record with the largest combined_clean_jsonb payload.
"""

import os
import sys
import logging
import traceback
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DB_HOST     = os.getenv("PG_HOST", "localhost")
DB_PORT     = int(os.getenv("PG_PORT", 5432))
DB_NAME     = os.getenv("PG_DB", "postgres")
DB_USER     = os.getenv("PG_USER", "postgres")
DB_PASSWORD = os.getenv("PG_PASSWORD", "")

LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "masterlinkage_unique_creation.log")

EXPECTED_ROW_COUNT = 6295

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
DROP_SQL = """
DROP MATERIALIZED VIEW IF EXISTS drugdb.masterlinkage_unique;
DROP TABLE IF EXISTS drugdb.masterlinkage_unique;
"""

CREATE_SQL = """
CREATE TABLE drugdb.masterlinkage_unique AS
SELECT DISTINCT ON (
    COALESCE(
        combined_clean_jsonb -> 'openfda' -> 'drug_info' ->> 'generic_name',
        combined_clean_jsonb -> 'dailymed' -> 'drug_info' -> 'products' -> 0 ->> 'generic_name'
    )
)
    master_linkage_id,
    COALESCE(
        combined_clean_jsonb -> 'openfda' -> 'drug_info' ->> 'generic_name',
        combined_clean_jsonb -> 'dailymed' -> 'drug_info' -> 'products' -> 0 ->> 'generic_name'
    ) AS generic_name,
    combined_clean_jsonb
FROM public."DrugMasterLinkage"
WHERE master_linkage_id != 'fe3345af-85ee-58b3-9cca-33617a7457cf'
AND TRIM(COALESCE(
    combined_clean_jsonb -> 'openfda' -> 'drug_info' ->> 'generic_name',
    combined_clean_jsonb -> 'dailymed' -> 'drug_info' -> 'products' -> 0 ->> 'generic_name'
)) != ''
AND COALESCE(
    combined_clean_jsonb -> 'openfda' -> 'drug_info' ->> 'generic_name',
    combined_clean_jsonb -> 'dailymed' -> 'drug_info' -> 'products' -> 0 ->> 'generic_name'
) IS NOT NULL
ORDER BY
    COALESCE(
        combined_clean_jsonb -> 'openfda' -> 'drug_info' ->> 'generic_name',
        combined_clean_jsonb -> 'dailymed' -> 'drug_info' -> 'products' -> 0 ->> 'generic_name'
    ),
    LENGTH(combined_clean_jsonb::text) DESC
"""

INDEXES = [
    ("idx_masterlinkage_unique_generic",  "CREATE INDEX idx_masterlinkage_unique_generic  ON drugdb.masterlinkage_unique(generic_name)"),
    ("idx_masterlinkage_unique_linkage",  "CREATE INDEX idx_masterlinkage_unique_linkage  ON drugdb.masterlinkage_unique(master_linkage_id)"),
]

COUNT_SQL       = "SELECT COUNT(*) FROM drugdb.masterlinkage_unique"
DIRTY_COUNT_SQL = "SELECT COUNT(*) FROM drugdb.masterlinkage_unique WHERE generic_name IS NULL OR generic_name = ''"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    start = datetime.now()
    log.info("=" * 70)
    log.info("START  create_masterlinkage_unique  —  %s", start.isoformat(timespec="seconds"))
    log.info("Target: drugdb.masterlinkage_unique  |  host: %s  db: %s", DB_HOST, DB_NAME)
    log.info("=" * 70)

    conn = None
    index_statuses: list[tuple[str, str]] = []
    row_count = None
    dirty_count = None

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            connect_timeout=30,
        )
        conn.autocommit = False
        cur = conn.cursor()

        # ------------------------------------------------------------------
        # Step 2 — drop existing table
        # ------------------------------------------------------------------
        log.info("Step 2: Dropping existing table (if any) …")
        cur.execute(DROP_SQL)
        conn.commit()
        log.info("Step 2: Old table dropped (or did not exist).")

        # ------------------------------------------------------------------
        # Step 3 — create table
        # ------------------------------------------------------------------
        log.info("Step 3: Creating drugdb.masterlinkage_unique …")
        cur.execute(CREATE_SQL)
        conn.commit()
        log.info("Step 3: Table created successfully.")

    except Exception:
        log.error("Step 3 FAILED — rolling back transaction.")
        log.error(traceback.format_exc())
        if conn:
            conn.rollback()
        _log_summary(start, row_count, dirty_count, index_statuses, failed=True)
        return 1

    try:
        cur = conn.cursor()

        # ------------------------------------------------------------------
        # Step 4 — indexes
        # ------------------------------------------------------------------
        log.info("Step 4: Creating indexes …")
        for idx_name, idx_sql in INDEXES:
            try:
                cur.execute(idx_sql)
                conn.commit()
                log.info("  Index created: %s", idx_name)
                index_statuses.append((idx_name, "OK"))
            except Exception:
                conn.rollback()
                log.error("  Index FAILED: %s\n%s", idx_name, traceback.format_exc())
                index_statuses.append((idx_name, "FAILED"))

        # ------------------------------------------------------------------
        # Step 5 — verify
        # ------------------------------------------------------------------
        log.info("Step 5: Verifying row counts …")

        cur.execute(COUNT_SQL)
        row_count = cur.fetchone()[0]
        log.info("  Total rows: %d", row_count)
        if row_count != EXPECTED_ROW_COUNT:
            log.warning("  WARNING: expected %d rows, got %d", EXPECTED_ROW_COUNT, row_count)
        else:
            log.info("  Row count matches expected value (%d). ✓", EXPECTED_ROW_COUNT)

        cur.execute(DIRTY_COUNT_SQL)
        dirty_count = cur.fetchone()[0]
        log.info("  Dirty records (NULL/empty generic_name): %d", dirty_count)
        if dirty_count != 0:
            log.warning("  WARNING: %d dirty records found — expected 0", dirty_count)
        else:
            log.info("  No dirty records. ✓")

    except Exception:
        log.error("Verification step failed.")
        log.error(traceback.format_exc())
        _log_summary(start, row_count, dirty_count, index_statuses, failed=True)
        return 1

    finally:
        if conn:
            conn.close()

    _log_summary(start, row_count, dirty_count, index_statuses, failed=False)
    return 0


def _log_summary(
    start: datetime,
    row_count,
    dirty_count,
    index_statuses: list[tuple[str, str]],
    failed: bool,
) -> None:
    end = datetime.now()
    duration = end - start
    log.info("")
    log.info("=" * 70)
    log.info("SUMMARY")
    log.info("  Start time   : %s", start.isoformat(timespec="seconds"))
    log.info("  End time     : %s", end.isoformat(timespec="seconds"))
    log.info("  Duration     : %s", str(duration).split(".")[0])
    log.info("  Row count    : %s", row_count if row_count is not None else "N/A")
    log.info("  Dirty records: %s", dirty_count if dirty_count is not None else "N/A")
    for idx_name, status in index_statuses:
        log.info("  Index %-45s %s", idx_name, status)
    if failed:
        log.error("  RESULT       : FAILED")
    else:
        warnings = []
        if row_count is not None and row_count != EXPECTED_ROW_COUNT:
            warnings.append(f"row count {row_count} != expected {EXPECTED_ROW_COUNT}")
        if dirty_count:
            warnings.append(f"{dirty_count} dirty records")
        if any(s == "FAILED" for _, s in index_statuses):
            warnings.append("one or more indexes failed")
        if warnings:
            log.warning("  RESULT       : COMPLETED WITH WARNINGS — %s", "; ".join(warnings))
        else:
            log.info("  RESULT       : SUCCESS")
    log.info("=" * 70)


if __name__ == "__main__":
    sys.exit(main())
