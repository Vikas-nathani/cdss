"""
utils.py — Shared helpers: DB connection, logging setup, batch updater.
"""

import logging
import time
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional

import psycopg2
import psycopg2.extras

from config import get_db_config, LOGS_DIR, RETRY_ATTEMPTS, RETRY_BACKOFF_SECONDS

# IST offset: UTC+5:30
IST_OFFSET_SECONDS = 19800

# ── Timezone helper ───────────────────────────────────────────────────────────

def now_ist() -> datetime:
    """Return current time in IST (UTC+5:30) as a naive datetime."""
    import time as _time
    utc_ts = _time.time()
    return datetime.utcfromtimestamp(utc_ts + IST_OFFSET_SECONDS)

def fmt_ist(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = now_ist()
    return dt.strftime("%Y-%m-%d %H:%M:%S IST")


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logger(name: str, log_file: str) -> logging.Logger:
    """Create a file-only logger (tab-separated lines). Stdout handled via print()."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    # Avoid duplicate handlers if called multiple times (e.g. --step reruns)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    fh = logging.FileHandler(str(LOGS_DIR / log_file), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)
    return logger


# ── DB connection with retry ──────────────────────────────────────────────────

def connect_db(password_override: str = None) -> psycopg2.extensions.connection:
    cfg = get_db_config(password_override)
    last_exc = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            conn = psycopg2.connect(**cfg)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as e:
            last_exc = e
            if attempt < RETRY_ATTEMPTS:
                print(f"[WARN] DB connection attempt {attempt} failed: {e}. Retrying in {RETRY_BACKOFF_SECONDS}s...")
                time.sleep(RETRY_BACKOFF_SECONDS)
    raise RuntimeError(f"Could not connect to database after {RETRY_ATTEMPTS} attempts: {last_exc}")


# ── Batch UPDATE helper ───────────────────────────────────────────────────────

def batch_update_rxcui(
    conn: psycopg2.extensions.connection,
    updates: List[Tuple],  # [(rxcui, confidence, id), ...]
    batch_size: int,
    dry_run: bool,
    logger: logging.Logger,
) -> int:
    """
    Execute batched UPDATE drugdb.indian_brand_ingredient SET rxcui_in, match_confidence WHERE id.
    Returns total rows updated.
    """
    if not updates:
        return 0

    total_updated = 0
    for batch_start in range(0, len(updates), batch_size):
        batch = updates[batch_start: batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        if dry_run:
            logger.info(f"[DRY-RUN] Would update batch {batch_num}: {len(batch)} rows")
            total_updated += len(batch)
            continue
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    UPDATE drugdb.indian_brand_ingredient AS t
                    SET rxcui_in = v.rxcui, match_confidence = v.confidence
                    FROM (VALUES %s) AS v(rxcui, confidence, id)
                    WHERE t.id = v.id::integer AND t.rxcui_in IS NULL
                    """,
                    batch,
                    template="(%s, %s, %s)",
                    page_size=batch_size,
                )
                conn.commit()
            total_updated += len(batch)
            logger.info(f"Batch {batch_num}: updated {len(batch)} rows ({total_updated} total so far)")
        except psycopg2.Error as e:
            conn.rollback()
            ids = [row[2] for row in batch]
            logger.error(f"[ERROR] Batch {batch_num} failed (IDs {ids[0]}–{ids[-1]}): {e}")
    return total_updated


# ── Batch INSERT audit helper ─────────────────────────────────────────────────

def batch_insert_audit(
    conn: psycopg2.extensions.connection,
    audit_rows: List[Tuple],
    batch_size: int,
    dry_run: bool,
    skip_audit: bool,
    logger: logging.Logger,
) -> None:
    """
    Insert rows into drugdb.rxcui_resolution_audit.
    audit_rows: list of (ibi_id, brand_id, name_norm, stripped_name, synonym_used,
                         matched_str, rxcui, tty, step, confidence, run_id)
    """
    if skip_audit or dry_run or not audit_rows:
        return

    for batch_start in range(0, len(audit_rows), batch_size):
        batch = audit_rows[batch_start: batch_start + batch_size]
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO drugdb.rxcui_resolution_audit
                        (indian_brand_ingredient_id, indian_brand_id, ingredient_name_norm,
                         ingredient_name_stripped, synonym_used, matched_str, rxcui, tty_matched,
                         resolution_step, match_confidence, pipeline_run_id)
                    VALUES %s
                    """,
                    batch,
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    page_size=batch_size,
                )
                conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"[ERROR] Audit insert batch failed: {e}")
