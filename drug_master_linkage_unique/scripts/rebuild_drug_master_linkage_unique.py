#!/usr/bin/env python3
"""
Rebuilds drugdb.drug_master_linkage_unique.

Unique key: (generic_formulation, dosage_forms)
Expected rows: ~10,752

Reads SQL from:  sql/schemas/rebuild_drug_master_linkage_unique.sql
Logs to:         logs/rebuild_drug_master_linkage_unique.log  +  stdout
Credentials:     DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
                 (falls back to PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASSWORD)
"""

import os
import sys
import logging
import traceback
import time
import threading
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
SQL_FILE   = REPO_ROOT / "sql" / "rebuild_drug_master_linkage_unique.sql"
LOG_FILE   = REPO_ROOT / "logs" / "rebuild_drug_master_linkage_unique.log"

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
# Expected row count range
# ---------------------------------------------------------------------------
EXPECTED_MIN = 10_500
EXPECTED_MAX = 11_000

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)-7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w"),   # fresh log each run
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL parsing
# ---------------------------------------------------------------------------
def parse_statements(sql: str) -> list[tuple[str, str]]:
    """
    Split on ';', strip comment lines, return (label, executable_sql) pairs.
    label is derived from the first non-comment line of the block.
    """
    results = []
    for raw in sql.split(";"):
        exec_lines = [
            line for line in raw.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        cleaned = "\n".join(exec_lines).strip()
        if not cleaned:
            continue

        # Derive a short label from the first keyword line
        first = cleaned.upper().lstrip()
        if first.startswith("DROP"):
            label = "DROP"
        elif first.startswith("CREATE TABLE"):
            label = "CREATE TABLE"
        elif first.startswith("CREATE INDEX"):
            # Extract index name from cleaned SQL
            tokens = cleaned.split()
            idx_name = tokens[2] if len(tokens) > 2 else "unknown"
            label = f"CREATE INDEX {idx_name}"
        else:
            label = cleaned[:50]

        results.append((label, cleaned))
    return results


# ---------------------------------------------------------------------------
# Heartbeat — logs a "still running" message every 30 s
# ---------------------------------------------------------------------------
def heartbeat(label: str, t0: float) -> threading.Event:
    stop = threading.Event()

    def _run():
        while not stop.wait(30):
            log.info("  [%s] Still running ... elapsed: %.0fs", label, time.monotonic() - t0)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return stop


# ---------------------------------------------------------------------------
# Commit helper — logs before and after every commit
# ---------------------------------------------------------------------------
def commit(conn, label: str) -> None:
    log.info("  [%s] Committing transaction ...", label)
    conn.commit()
    log.info("  [%s] Transaction committed.", label)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    run_start = time.monotonic()

    log.info("=" * 72)
    log.info("  JOB    : rebuild_drug_master_linkage_unique")
    log.info("  SQL    : %s", SQL_FILE)
    log.info("  HOST   : %s:%s", DB_HOST, DB_PORT)
    log.info("  DB     : %s    USER: %s", DB_NAME, DB_USER)
    log.info("  EXPECT : %d – %d rows", EXPECTED_MIN, EXPECTED_MAX)
    log.info("=" * 72)

    if not SQL_FILE.exists():
        log.error("SQL file not found: %s", SQL_FILE)
        return 1

    statements = parse_statements(SQL_FILE.read_text())
    log.info("Parsed %d executable statements from SQL file.", len(statements))
    log.info("")

    conn = None
    try:
        log.info("Connecting to PostgreSQL ...")
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            connect_timeout=30,
        )
        conn.autocommit = False
        cur = conn.cursor()
        log.info("Connected.  server_version=%s",
                 conn.server_version)
        log.info("")

        for idx, (label, stmt) in enumerate(statements, 1):
            upper = stmt.upper().lstrip()

            # ------------------------------------------------------------------
            # Step 1 — DROP
            # ------------------------------------------------------------------
            if upper.startswith("DROP"):
                log.info("─" * 72)
                log.info("  STEP 1 of %d  │  %s", len(statements), label)
                log.info("  Action : Drop existing table if it exists")
                log.info("  SQL    : %s", stmt.splitlines()[0])
                log.info("─" * 72)

                t0 = time.monotonic()
                log.info("  [DROP] Executing ...")
                cur.execute(stmt)
                commit(conn, "DROP")
                log.info("  [DROP] Completed in %.2fs — old table removed (or did not exist).",
                         time.monotonic() - t0)
                log.info("")

            # ------------------------------------------------------------------
            # Step 2 — CREATE TABLE
            # ------------------------------------------------------------------
            elif upper.startswith("CREATE TABLE"):
                log.info("─" * 72)
                log.info("  STEP 2 of %d  │  %s", len(statements), label)
                log.info("  Action : Build new table via DISTINCT ON")
                log.info("           (generic_formulation, dosage_forms)")
                log.info("  Note   : JSONB join — this may take several minutes ...")
                log.info("  Expect : ~10,752 rows")
                log.info("─" * 72)

                t0 = time.monotonic()
                log.info("  [CREATE TABLE] Executing ...")
                hb_stop = heartbeat("CREATE TABLE", t0)
                try:
                    cur.execute(stmt)
                    commit(conn, "CREATE TABLE")
                finally:
                    hb_stop.set()

                elapsed = time.monotonic() - t0
                cur.execute("SELECT COUNT(*) FROM drugdb.drug_master_linkage_unique")
                row_count = cur.fetchone()[0]
                log.info("  [CREATE TABLE] Completed in %.2fs", elapsed)
                log.info("  [CREATE TABLE] Rows inserted : %d", row_count)
                log.info("")

            # ------------------------------------------------------------------
            # Step 3 — CREATE INDEX
            # ------------------------------------------------------------------
            elif upper.startswith("CREATE INDEX"):
                step_num = idx   # steps 3–8
                log.info("─" * 72)
                log.info("  STEP %d of %d  │  %s", step_num, len(statements), label)
                log.info("─" * 72)

                t0 = time.monotonic()
                log.info("  [%s] Executing ...", label)
                cur.execute(stmt)
                commit(conn, label)
                log.info("  [%s] Completed in %.2fs", label, time.monotonic() - t0)
                log.info("")

            # ------------------------------------------------------------------
            # Fallback
            # ------------------------------------------------------------------
            else:
                log.info("  Executing: %s", stmt[:80])
                cur.execute(stmt)
                commit(conn, "misc")
                log.info("")

        # ----------------------------------------------------------------------
        # Final verification
        # ----------------------------------------------------------------------
        log.info("=" * 72)
        log.info("  VERIFICATION")
        log.info("=" * 72)
        log.info("  Query  : SELECT COUNT(*) FROM drugdb.drug_master_linkage_unique")
        cur.execute("SELECT COUNT(*) FROM drugdb.drug_master_linkage_unique")
        final_count = cur.fetchone()[0]
        log.info("  Result : %d rows", final_count)

        if EXPECTED_MIN <= final_count <= EXPECTED_MAX:
            log.info("  Check  : PASS  (within expected range %d–%d)",
                     EXPECTED_MIN, EXPECTED_MAX)
        else:
            log.warning("  Check  : WARNING — %d rows is outside expected range %d–%d",
                        final_count, EXPECTED_MIN, EXPECTED_MAX)
        log.info("")

    except Exception:
        log.error("=" * 72)
        log.error("  FATAL ERROR — rolling back all changes")
        log.error("=" * 72)
        log.error(traceback.format_exc())
        if conn:
            try:
                conn.rollback()
                log.error("  Rollback complete.")
            except Exception:
                log.error("  Rollback also failed.")
        _log_footer(run_start, success=False)
        return 1

    finally:
        if conn:
            conn.close()
            log.info("  Database connection closed.")

    _log_footer(run_start, success=True)
    return 0


def _log_footer(run_start: float, success: bool) -> None:
    elapsed = time.monotonic() - run_start
    m, s = divmod(int(elapsed), 60)
    status = "SUCCESS" if success else "FAILED"
    log.info("=" * 72)
    log.info("  RESULT        : %s", status)
    log.info("  TOTAL ELAPSED : %dm %02ds", m, s)
    log.info("=" * 72)


if __name__ == "__main__":
    sys.exit(main())
