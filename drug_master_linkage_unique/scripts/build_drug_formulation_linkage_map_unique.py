#!/usr/bin/env python3
"""
Builds drugdb.drug_formulation_linkage_map_unique.

Purpose : 1-to-1 map: formulation_id ↔ master_linkage_id
          resolved by joining on (master_linkage_id, generic_formulation, dosage_forms)
Expected: ~10,752 rows

SQL file : sql/schemas/build_drug_formulation_linkage_map_unique.sql
Log file : logs/build_drug_formulation_linkage_map_unique.log
Creds    : DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
           (fallback: PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASSWORD)
"""

import os
import sys
import logging
import traceback
import time
import datetime
import platform
from pathlib import Path

import psycopg2
import psycopg2.extras

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
SQL_FILE   = REPO_ROOT / "sql" / "build_drug_formulation_linkage_map_unique.sql"
LOG_FILE   = REPO_ROOT / "logs" / "build_drug_formulation_linkage_map_unique.log"

load_dotenv(REPO_ROOT / ".env")

# ---------------------------------------------------------------------------
# Credentials — no hardcoded values
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv("DB_HOST")     or os.getenv("PG_HOST")
DB_PORT     = int(os.getenv("DB_PORT") or os.getenv("PG_PORT") or 5432)
DB_NAME     = os.getenv("DB_NAME")     or os.getenv("PG_DB")
DB_USER     = os.getenv("DB_USER")     or os.getenv("PG_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("PG_PASSWORD")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TABLE        = "drugdb.drug_formulation_linkage_map_unique"
EXPECTED_MIN = 10_000
EXPECTED_MAX = 12_000

# ---------------------------------------------------------------------------
# Logging — DEBUG to file, INFO to console
# ---------------------------------------------------------------------------
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

_fmt = "[%(asctime)s] [%(levelname)-7s] %(message)s"
_datefmt = "%Y-%m-%dT%H:%M:%S%z"

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(_fmt, datefmt=_datefmt))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(_fmt, datefmt=_datefmt))

logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, console_handler])
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL parsing — returns list of (label, sql_text)
# ---------------------------------------------------------------------------
def parse_statements(sql: str) -> list[tuple[str, str]]:
    results = []
    for raw in sql.split(";"):
        exec_lines = [
            ln for ln in raw.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        cleaned = "\n".join(exec_lines).strip()
        if not cleaned:
            continue

        upper = cleaned.upper().lstrip()
        if upper.startswith("DROP"):
            label = "DROP TABLE"
        elif upper.startswith("CREATE TABLE"):
            label = "CREATE TABLE"
        elif upper.startswith("INSERT"):
            label = "INSERT"
        elif upper.startswith("ALTER TABLE"):
            label = "ALTER TABLE (UNIQUE CONSTRAINT)"
        elif upper.startswith("CREATE INDEX"):
            tokens = cleaned.split()
            label = f"CREATE INDEX {tokens[2]}" if len(tokens) > 2 else "CREATE INDEX"
        elif upper.startswith("SELECT"):
            # Derive a short tag from the AS alias or first column
            if "total_rows" in cleaned.lower():
                label = "SELECT total_rows"
            elif "duplicate_formulation_ids" in cleaned.lower():
                label = "SELECT duplicate_formulation_ids"
            elif "unique_master_linkage_ids" in cleaned.lower():
                label = "SELECT unique_master_linkage_ids"
            else:
                label = "SELECT"
        else:
            label = cleaned[:50]

        results.append((label, cleaned))
    return results


def ts() -> str:
    """ISO timestamp with timezone for inline log messages."""
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    run_start = time.monotonic()
    start_ts  = ts()

    # -- Environment banner --------------------------------------------------
    log.info("=" * 72)
    log.info("  JOB       : build_drug_formulation_linkage_map_unique")
    log.info("  STARTED   : %s", start_ts)
    log.info("  SQL FILE  : %s", SQL_FILE)
    log.info("  LOG FILE  : %s", LOG_FILE)
    log.info("  HOST      : %s:%s", DB_HOST, DB_PORT)
    log.info("  DATABASE  : %s", DB_NAME)
    log.info("  USER      : %s", DB_USER)
    log.info("  EXPECT    : %d – %d rows", EXPECTED_MIN, EXPECTED_MAX)
    log.info("=" * 72)

    log.debug("  Python    : %s", sys.version.replace("\n", " "))
    log.debug("  psycopg2  : %s", psycopg2.__version__)
    log.debug("  Platform  : %s", platform.platform())
    log.info("")

    if not SQL_FILE.exists():
        log.error("SQL file not found: %s", SQL_FILE)
        return 1

    statements = parse_statements(SQL_FILE.read_text())
    log.info("Parsed %d executable statements from SQL file.", len(statements))
    for i, (lbl, _) in enumerate(statements, 1):
        log.debug("  Statement %d: %s", i, lbl)
    log.info("")

    # -- Tracking state ------------------------------------------------------
    rows_inserted     = None
    total_rows        = None
    dup_ids           = None
    unique_linkages   = None
    indexes_created   = 0
    indexes_total     = sum(1 for lbl, _ in statements if lbl.startswith("CREATE INDEX"))

    conn = None
    current_step = "CONNECT"

    try:
        log.info("Connecting to PostgreSQL ...")
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            connect_timeout=30,
        )
        conn.autocommit = False
        cur = conn.cursor()
        log.info("Connected.  server_version=%s", conn.server_version)
        log.info("")

        # ======================================================================
        # PHASE 1 — Single transaction: DROP + CREATE + INSERT
        # ======================================================================
        log.info("─" * 72)
        log.info("  PHASE 1  │  DROP → CREATE → INSERT  (single transaction)")
        log.info("─" * 72)

        for label, stmt in statements:

            # ------------------------------------------------------------------
            # STEP 1 — DROP
            # ------------------------------------------------------------------
            if label == "DROP TABLE":
                current_step = "DROP TABLE"
                log.info("")
                log.info("  [STEP 1]  DROP TABLE %s", TABLE)
                log.info("  Starting DROP TABLE %s ...", TABLE)
                log.debug("  SQL: %s", stmt)
                t0 = time.monotonic()
                cur.execute(stmt)
                log.info("  DROP TABLE completed successfully.  [%s]  elapsed=%.2fs",
                          ts(), time.monotonic() - t0)

            # ------------------------------------------------------------------
            # STEP 2 — CREATE TABLE
            # ------------------------------------------------------------------
            elif label == "CREATE TABLE":
                current_step = "CREATE TABLE"
                log.info("")
                log.info("  [STEP 2]  CREATE TABLE %s", TABLE)
                log.info("  Creating table with SERIAL PK + 7 columns ...")
                log.debug("  SQL:\n%s", stmt)
                t0 = time.monotonic()
                cur.execute(stmt)
                log.info("  CREATE TABLE completed successfully.  [%s]  elapsed=%.2fs",
                          ts(), time.monotonic() - t0)

            # ------------------------------------------------------------------
            # STEP 3 — INSERT
            # ------------------------------------------------------------------
            elif label == "INSERT":
                current_step = "INSERT"
                log.info("")
                log.info("  [STEP 3]  INSERT data")
                log.info("  Starting data insert from drug_master_linkage_unique JOIN drugdb.drug ...")
                log.info("  Expected ~10,752 rows ...")
                log.debug("  SQL:\n%s", stmt)
                t0 = time.monotonic()
                cur.execute(stmt)
                rows_inserted = cur.rowcount
                elapsed = time.monotonic() - t0

                log.info("  Data insert completed.  [%s]  elapsed=%.2fs", ts(), elapsed)
                log.info("  Rows inserted: %d", rows_inserted)

                if not (EXPECTED_MIN <= rows_inserted <= EXPECTED_MAX):
                    log.warning(
                        "  Unexpected row count: %d. Expected between %d and %d.",
                        rows_inserted, EXPECTED_MIN, EXPECTED_MAX
                    )
                else:
                    log.info("  Row count within expected range (%d–%d). ✓",
                             EXPECTED_MIN, EXPECTED_MAX)

                # COMMIT the transaction here, before indexes/constraints
                current_step = "COMMIT"
                log.info("")
                log.info("  Committing transaction (DROP + CREATE + INSERT) ...")
                conn.commit()
                log.info("  Transaction committed successfully.  [%s]", ts())

            # Skip the remaining statement types in Phase 1
            elif label in ("ALTER TABLE (UNIQUE CONSTRAINT)",) or \
                 label.startswith("CREATE INDEX") or \
                 label.startswith("SELECT"):
                continue

        # ======================================================================
        # PHASE 2 — Post-commit: UNIQUE CONSTRAINT + INDEXES (autocommit)
        # ======================================================================
        log.info("")
        log.info("─" * 72)
        log.info("  PHASE 2  │  UNIQUE CONSTRAINT + INDEXES  (autocommit)")
        log.info("─" * 72)
        conn.autocommit = True

        for label, stmt in statements:

            # ------------------------------------------------------------------
            # STEP 4 — UNIQUE CONSTRAINT
            # ------------------------------------------------------------------
            if label == "ALTER TABLE (UNIQUE CONSTRAINT)":
                current_step = "UNIQUE CONSTRAINT"
                log.info("")
                log.info("  [STEP 4]  ADD UNIQUE CONSTRAINT (formulation_id, master_linkage_id)")
                log.info("  Adding UNIQUE constraint on (formulation_id, master_linkage_id) ...")
                log.debug("  SQL: %s", stmt)
                t0 = time.monotonic()
                try:
                    cur.execute(stmt)
                    log.info("  UNIQUE constraint added successfully.  [%s]  elapsed=%.2fs",
                              ts(), time.monotonic() - t0)
                except psycopg2.errors.UniqueViolation as e:
                    # Find the duplicates and log them before raising
                    log.error("  UNIQUE CONSTRAINT FAILED — duplicate (formulation_id, master_linkage_id) pairs exist.")
                    log.error("  psycopg2 error: %s", str(e))
                    try:
                        dup_cur = conn.cursor()
                        dup_cur.execute("""
                            SELECT formulation_id, master_linkage_id, COUNT(*) AS cnt
                            FROM drugdb.drug_formulation_linkage_map_unique
                            GROUP BY formulation_id, master_linkage_id
                            HAVING COUNT(*) > 1
                            ORDER BY cnt DESC
                            LIMIT 20
                        """)
                        dups = dup_cur.fetchall()
                        log.error("  Top duplicate pairs (up to 20):")
                        for row in dups:
                            log.error("    formulation_id=%s  master_linkage_id=%s  count=%d",
                                      row[0], row[1], row[2])
                    except Exception:
                        log.error("  Could not fetch duplicate details.")
                    raise

            # ------------------------------------------------------------------
            # STEP 5 — INDEXES
            # ------------------------------------------------------------------
            elif label.startswith("CREATE INDEX"):
                current_step = label
                idx_name = label.replace("CREATE INDEX ", "")
                log.info("")
                log.info("  [STEP 5]  %s", label)
                log.info("  Creating index %s ...", idx_name)
                log.debug("  SQL: %s", stmt)
                t0 = time.monotonic()
                cur.execute(stmt)
                elapsed = time.monotonic() - t0
                indexes_created += 1
                log.info("  Index %s created successfully.  Time taken: %.2fs  [%s]",
                         idx_name, elapsed, ts())

        # ======================================================================
        # PHASE 3 — VERIFICATION
        # ======================================================================
        log.info("")
        log.info("─" * 72)
        log.info("  PHASE 3  │  VERIFICATION QUERIES")
        log.info("─" * 72)

        for label, stmt in statements:
            if not label.startswith("SELECT"):
                continue

            current_step = f"VERIFY ({label})"
            log.debug("  SQL: %s", stmt.replace("\n", " "))
            cur.execute(stmt)
            result = cur.fetchone()

            if label == "SELECT total_rows":
                total_rows = result[0]
                log.info("")
                log.info("  Total rows in mapping table  : %d", total_rows)

            elif label == "SELECT duplicate_formulation_ids":
                dup_ids = result[0]
                log.info("  Duplicate formulation_ids    : %d", dup_ids)
                if dup_ids > 0:
                    log.error("  DATA INTEGRITY ISSUE: duplicate formulation_ids detected! "
                              "Count: %d", dup_ids)
                else:
                    log.info("  No duplicate formulation_ids. ✓")

            elif label == "SELECT unique_master_linkage_ids":
                unique_linkages = result[0]
                log.info("  Unique master_linkage_ids    : %d", unique_linkages)
                if unique_linkages and total_rows:
                    avg = total_rows / unique_linkages
                    log.info("  One master_linkage_id covers an average of %.1f formulations.", avg)

    except Exception:
        log.error("")
        log.error("=" * 72)
        log.error("  FAILED at step: %s", current_step)
        log.error("=" * 72)
        log.error(traceback.format_exc())
        if conn and not conn.autocommit:
            log.error("  Rolling back transaction ...")
            try:
                conn.rollback()
                log.error("  Rollback completed. Table was not created.")
            except Exception:
                log.error("  Rollback also failed:\n%s", traceback.format_exc())
        _log_summary(
            run_start=run_start, start_ts=start_ts,
            success=False,
            rows_inserted=rows_inserted, total_rows=total_rows,
            unique_linkages=unique_linkages,
            indexes_created=indexes_created, indexes_total=indexes_total,
        )
        return 1

    finally:
        if conn:
            conn.close()
            log.debug("  Database connection closed.")

    _log_summary(
        run_start=run_start, start_ts=start_ts,
        success=True,
        rows_inserted=rows_inserted, total_rows=total_rows,
        unique_linkages=unique_linkages,
        indexes_created=indexes_created, indexes_total=indexes_total,
    )
    return 0


def _log_summary(
    run_start: float,
    start_ts: str,
    success: bool,
    rows_inserted,
    total_rows,
    unique_linkages,
    indexes_created: int,
    indexes_total: int,
) -> None:
    elapsed = time.monotonic() - run_start
    m, s = divmod(int(elapsed), 60)
    status = "SUCCESS" if success else "FAILED"

    log.info("")
    log.info("=" * 72)
    log.info("  SUMMARY")
    log.info("=" * 72)
    log.info("  Status          : %s", status)
    log.info("  Table           : %s", TABLE)
    log.info("  Started at      : %s", start_ts)
    log.info("  Finished at     : %s", ts())
    log.info("  Rows inserted   : %s", rows_inserted if rows_inserted is not None else "N/A")
    log.info("  Verified rows   : %s", total_rows    if total_rows    is not None else "N/A")
    log.info("  Unique linkages : %s", unique_linkages if unique_linkages is not None else "N/A")
    log.info("  Indexes created : %d / %d", indexes_created, indexes_total)
    log.info("  Total time      : %dm %02ds", m, s)
    log.info("  Log file        : %s", LOG_FILE)
    log.info("=" * 72)


if __name__ == "__main__":
    sys.exit(main())
