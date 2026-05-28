#!/usr/bin/env python3
"""
enrich_sibling_fill.py — Sibling-based JSON enrichment for drug_master_linkage_unique.

For each row in drugdb.drug_master_linkage_unique, fills null/empty fields in
combined_clean_jsonb using non-null values from sibling mLIds that share the same
(generic_formulation, dosage_forms) in public."DrugMasterLinkage".

Fill rule: longest serialized text wins; smallest mLId (lexicographic) breaks ties.
Arrays are treated as atomic units — replaced wholesale, never element-merged.

Prerequisites:
    psql -h 178.236.185.230 -U postgres -d postgres -f setup_schema.sql

Usage:
    python enrich_sibling_fill.py --mode=dry-run  [--batch-size=500]
    python enrich_sibling_fill.py --mode=full-run [--batch-size=500]

Environment variables (override built-in defaults):
    DATABASE_URL          full DSN, e.g. postgresql://user:pass@host:5432/dbname
    DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD   individual components
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import logging.handlers
import os
import sys
import time
import traceback
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from tqdm import tqdm

# ── Path constants ─────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Table name constants ───────────────────────────────────────────────────────

TARGET_TABLE = "drugdb.drug_master_linkage_unique"
AUDIT_TABLE = "drugdb.drug_master_linkage_enrichment_audit"
SOURCE_TABLE = 'public."DrugMasterLinkage"'
DRUG_TABLE = "drugdb.drug"

# ── Data classes ───────────────────────────────────────────────────────────────


@dataclass
class AuditRow:
    """One audit record per field per enriched mLId per run."""

    run_id: str
    run_mode: str                  # 'dry_run' | 'full_run'
    target_mlid: str
    generic_formulation: str
    dosage_form: str
    field_path: str
    original_value: Any
    filled_value: Any
    original_value_length: Optional[int]
    filled_value_length: Optional[int]
    source_sibling_mlid: str       # empty string when not filled
    sibling_count: int
    status: str                    # 'filled' | 'skipped_no_sibling_value' | 'error'
    error_message: Optional[str]


@dataclass
class RecordResult:
    """Outcome of processing a single target row."""

    target_mlid: str
    generic_formulation: str
    dosage_form: str
    sibling_count: int
    fields_filled: list[str]
    fields_still_null: list[str]
    audit_rows: list[AuditRow]
    enriched_json: Optional[dict]
    status: str                    # 'ok' | 'skipped' | 'error'
    error: Optional[str]
    error_traceback: Optional[str]


@dataclass
class RunStats:
    """Aggregate counters updated during the run."""

    total_processed: int = 0
    total_enriched: int = 0            # records with ≥1 field filled
    total_fields_filled: int = 0
    total_errors: int = 0
    total_remaining_null_records: int = 0
    field_fill_counts: dict = field(default_factory=dict)
    field_skip_counts: dict = field(default_factory=dict)  # skipped_no_sibling_value


# ── JSON utilities ─────────────────────────────────────────────────────────────


def is_empty(value: Any) -> bool:
    """True for None, whitespace-only string, empty list, empty dict."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, list) and len(value) == 0:
        return True
    if isinstance(value, dict) and len(value) == 0:
        return True
    return False


def json_text_len(value: Any) -> int:
    """Serialized JSON character length used for longest-text comparison."""
    if value is None:
        return 0
    return len(json.dumps(value, ensure_ascii=False))


def get_at_path(obj: Any, path_parts: list[str]) -> Any:
    """
    Navigate a nested dict by a list of key parts.
    Returns None if any intermediate key is missing or not a dict.
    """
    current = obj
    for part in path_parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def walk_and_fill(
    target: dict,
    siblings: list[tuple[str, dict]],
    path_parts: list[str],
    run_id: str,
    run_mode: str,
    target_mlid: str,
    generic_formulation: str,
    dosage_form: str,
) -> list[AuditRow]:
    """
    Recursively walk *target* (modified in-place), filling empty fields from siblings.

    Rules applied per field:
      - is_empty(value)       → gap: attempt fill from best sibling; emit audit row
      - non-empty dict        → recurse into sub-keys
      - non-empty list        → skip (arrays are atomic units; only empty arrays are gaps)
      - non-empty scalar      → skip

    Sibling selection: longest serialized text wins; lexicographically smallest
    sibling mLId breaks ties.

    Returns a list of AuditRow objects (filled + skipped_no_sibling_value entries).
    """
    audit_rows: list[AuditRow] = []

    for key in list(target.keys()):
        value = target[key]
        current_parts = path_parts + [key]
        field_path = ".".join(current_parts)

        if is_empty(value):
            candidates: list[tuple[str, Any]] = []
            for sib_mlid, sib_json in siblings:
                sib_val = get_at_path(sib_json, current_parts)
                if not is_empty(sib_val):
                    candidates.append((sib_mlid, sib_val))

            if candidates:
                winner_mlid, winner_val = sorted(
                    candidates,
                    key=lambda x: (-json_text_len(x[1]), x[0]),
                )[0]
                target[key] = winner_val
                audit_rows.append(
                    AuditRow(
                        run_id=run_id,
                        run_mode=run_mode,
                        target_mlid=target_mlid,
                        generic_formulation=generic_formulation,
                        dosage_form=dosage_form,
                        field_path=field_path,
                        original_value=value,
                        filled_value=winner_val,
                        original_value_length=json_text_len(value),
                        filled_value_length=json_text_len(winner_val),
                        source_sibling_mlid=winner_mlid,
                        sibling_count=len(candidates),
                        status="filled",
                        error_message=None,
                    )
                )
            else:
                audit_rows.append(
                    AuditRow(
                        run_id=run_id,
                        run_mode=run_mode,
                        target_mlid=target_mlid,
                        generic_formulation=generic_formulation,
                        dosage_form=dosage_form,
                        field_path=field_path,
                        original_value=value,
                        filled_value=None,
                        original_value_length=json_text_len(value),
                        filled_value_length=None,
                        source_sibling_mlid="",
                        sibling_count=0,
                        status="skipped_no_sibling_value",
                        error_message=None,
                    )
                )

        elif isinstance(value, dict) and value:
            # Non-empty object: recurse into sub-fields
            sub_rows = walk_and_fill(
                target[key],
                siblings,
                current_parts,
                run_id,
                run_mode,
                target_mlid,
                generic_formulation,
                dosage_form,
            )
            audit_rows.extend(sub_rows)

        # Non-empty list or scalar: skip entirely

    return audit_rows


def find_null_paths(obj: Any, path_parts: list[str]) -> list[str]:
    """
    Return dot-separated paths to all empty/null leaves in a JSON structure.
    Recurses into non-empty dicts; treats non-empty arrays as atomic (not a null path).
    """
    paths: list[str] = []
    if not isinstance(obj, dict):
        return paths
    for key, value in obj.items():
        current_parts = path_parts + [key]
        if is_empty(value):
            paths.append(".".join(current_parts))
        elif isinstance(value, dict) and value:
            paths.extend(find_null_paths(value, current_parts))
    return paths


# ── Database helpers ───────────────────────────────────────────────────────────


def get_connection() -> psycopg2.extensions.connection:
    """
    Build a psycopg2 connection.

    Priority: DATABASE_URL env var → individual DB_* env vars → hardcoded defaults.
    """
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)

    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", ""),
    )


def check_prior_dry_run(conn: psycopg2.extensions.connection) -> bool:
    """Return True if any dry_run rows exist in the audit table."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {AUDIT_TABLE} WHERE run_mode = 'dry_run' LIMIT 1"
        )
        return cur.fetchone() is not None


def count_target_rows(conn: psycopg2.extensions.connection) -> int:
    """Return total row count of the target table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
        row = cur.fetchone()
        return row[0] if row else 0


def fetch_batch(
    conn: psycopg2.extensions.connection, offset: int, batch_size: int
) -> list[dict]:
    """
    Fetch one page of rows from drug_master_linkage_unique.
    Always reads from combined_clean_jsonb (source of truth), not unified_json_enriched,
    so re-runs are fully idempotent.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                master_linkage_id::text  AS master_linkage_id,
                generic_formulation,
                dosage_forms,
                combined_clean_jsonb
            FROM {TARGET_TABLE}
            ORDER BY master_linkage_id
            LIMIT %s OFFSET %s
            """,
            (batch_size, offset),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_siblings_for_pairs(
    conn: psycopg2.extensions.connection,
    pairs: list[tuple[str, str]],
) -> dict[tuple[str, str], list[tuple[str, dict]]]:
    """
    Single-query fetch: for every (generic_formulation, dosage_forms) pair in *pairs*,
    return all (mLId, combined_clean_jsonb) tuples from DrugMasterLinkage, grouped by pair.

    Includes the chosen mLId itself — callers must exclude it when building sibling lists.

    Uses a VALUES join to pass the batch of pairs efficiently without scanning the full
    drug table for each pair individually.
    """
    if not pairs:
        return {}

    with conn.cursor() as cur:
        values_sql = ", ".join(
            cur.mogrify("(%s, %s)", pair).decode() for pair in pairs
        )
        cur.execute(
            f"""
            SELECT
                d.generic_formulation,
                d.dosage_forms,
                dml.master_linkage_id::text  AS mlid,
                dml.combined_clean_jsonb
            FROM {DRUG_TABLE} d
            JOIN {SOURCE_TABLE} dml
                ON d.master_linkage_id = dml.master_linkage_id
            JOIN (VALUES {values_sql}) AS v(gf, df)
                ON d.generic_formulation = v.gf
               AND d.dosage_forms        = v.df
            WHERE d.master_linkage_id    IS NOT NULL
              AND dml.combined_clean_jsonb IS NOT NULL
            """
        )
        result: dict[tuple[str, str], list[tuple[str, dict]]] = defaultdict(list)
        for row in cur.fetchall():
            gf, df, mlid, jsonb_val = row
            result[(gf, df)].append((mlid, jsonb_val))
        return dict(result)


def server_side_update_enriched_json(
    conn: psycopg2.extensions.connection,
    run_id: str,
) -> int:
    """
    Single server-side UPDATE that reads filled values from the audit table
    and applies them to combined_clean_jsonb using drugdb.apply_json_fills().

    No enriched JSONBs are transferred from Python — only the run_id travels
    over the network. Returns the number of rows updated.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {TARGET_TABLE} AS u
            SET    unified_json_enriched = drugdb.apply_json_fills(
                       u.combined_clean_jsonb,
                       fills.path_values
                   )
            FROM (
                SELECT target_mlid,
                       jsonb_object_agg(field_path, filled_value) AS path_values
                FROM   {AUDIT_TABLE}
                WHERE  run_id = %s
                  AND  status = 'filled'
                GROUP  BY target_mlid
            ) AS fills
            WHERE u.master_linkage_id::text = fills.target_mlid
            """,
            (run_id,),
        )
        return cur.rowcount


def bulk_insert_audit(
    conn: psycopg2.extensions.connection,
    audit_rows: list[AuditRow],
) -> None:
    """Bulk INSERT all audit rows for a batch using execute_values."""
    if not audit_rows:
        return

    def _jsonb(v: Any) -> Optional[str]:
        return json.dumps(v, ensure_ascii=False) if v is not None else None

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
            INSERT INTO {AUDIT_TABLE}
                (run_id, run_mode, target_mlid, generic_formulation, dosage_form,
                 field_path, original_value, filled_value,
                 original_value_length, filled_value_length,
                 source_sibling_mlid, sibling_count, status, error_message)
            VALUES %s
            """,
            [
                (
                    r.run_id,
                    r.run_mode,
                    r.target_mlid,
                    r.generic_formulation,
                    r.dosage_form,
                    r.field_path,
                    _jsonb(r.original_value),
                    _jsonb(r.filled_value),
                    r.original_value_length,
                    r.filled_value_length,
                    r.source_sibling_mlid,
                    r.sibling_count,
                    r.status,
                    r.error_message,
                )
                for r in audit_rows
            ],
        )


# ── Logging setup ──────────────────────────────────────────────────────────────


class _TeeWriter:
    """Mirror writes to both the original stream and a log file (captures tqdm too)."""

    def __init__(self, original, logfile):
        self._original = original
        self._logfile = logfile

    def write(self, data):
        self._original.write(data)
        self._logfile.write(data)

    def flush(self):
        self._original.flush()
        self._logfile.flush()

    def fileno(self):
        return self._original.fileno()

    def isatty(self):
        return self._original.isatty()


class _PlainFormatter(logging.Formatter):
    """Pass through the message string unchanged — caller pre-formats JSON lines."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        return record.getMessage()


def _make_file_logger(name: str, filepath: Path) -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    log.propagate = False
    handler = logging.handlers.RotatingFileHandler(
        filepath,
        maxBytes=200 * 1024 * 1024,  # 200 MB safety cap per file
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(_PlainFormatter())
    log.addHandler(handler)
    return log


def setup_run_loggers(
    run_id: str,
) -> tuple[logging.Logger, logging.Logger, logging.Logger, logging.Logger]:
    """
    Create four per-run log files:
      summary  → run_<run_id>_summary.log   (high-level, written at end)
      detailed → run_<run_id>_detailed.log  (one JSON line per record)
      errors   → run_<run_id>_errors.log    (one JSON line per error)
      sections → run_<run_id>_sections.log  (one JSON line per section path: filled + not_filled)
    """
    summary = _make_file_logger(
        f"summary_{run_id}", LOG_DIR / f"run_{run_id}_summary.log"
    )
    detailed = _make_file_logger(
        f"detailed_{run_id}", LOG_DIR / f"run_{run_id}_detailed.log"
    )
    errors = _make_file_logger(
        f"errors_{run_id}", LOG_DIR / f"run_{run_id}_errors.log"
    )
    sections = _make_file_logger(
        f"sections_{run_id}", LOG_DIR / f"run_{run_id}_sections.log"
    )
    return summary, detailed, errors, sections


def _console_logger() -> logging.Logger:
    log = logging.getLogger("enrich_console")
    if not log.handlers:
        log.setLevel(logging.INFO)
        log.propagate = False
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)
        sh.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        )
        log.addHandler(sh)
    return log


# ── Record processing ──────────────────────────────────────────────────────────


def process_record(
    row: dict,
    siblings_map: dict[tuple[str, str], list[tuple[str, dict]]],
    run_id: str,
    run_mode_db: str,
) -> RecordResult:
    """
    Enrich one target row.

    1. Deep-copy combined_clean_jsonb (source of truth; never mutated).
    2. Collect sibling JSONs for the same (generic_formulation, dosage_forms),
       excluding the target mLId itself.
    3. Walk the copy, filling empty fields from siblings in-place.
    4. Scan the result for any remaining null paths.
    5. Return a RecordResult with the enriched JSON and all audit rows.
    """
    target_mlid = row["master_linkage_id"]
    gf: str = row.get("generic_formulation") or ""
    df: str = row.get("dosage_forms") or ""

    source_json = row.get("combined_clean_jsonb")

    if not isinstance(source_json, dict) or not source_json:
        return RecordResult(
            target_mlid=target_mlid,
            generic_formulation=gf,
            dosage_form=df,
            sibling_count=0,
            fields_filled=[],
            fields_still_null=[],
            audit_rows=[],
            enriched_json=source_json,
            status="skipped",
            error=None,
            error_traceback=None,
        )

    all_for_pair = siblings_map.get((gf, df), [])
    siblings = [(mlid, j) for mlid, j in all_for_pair if mlid != target_mlid]

    enriched = copy.deepcopy(source_json)

    audit_rows = walk_and_fill(
        target=enriched,
        siblings=siblings,
        path_parts=[],
        run_id=run_id,
        run_mode=run_mode_db,
        target_mlid=target_mlid,
        generic_formulation=gf,
        dosage_form=df,
    )

    filled_paths = [r.field_path for r in audit_rows if r.status == "filled"]
    still_null_paths = find_null_paths(enriched, [])

    return RecordResult(
        target_mlid=target_mlid,
        generic_formulation=gf,
        dosage_form=df,
        sibling_count=len(siblings),
        fields_filled=filled_paths,
        fields_still_null=still_null_paths,
        audit_rows=audit_rows,
        enriched_json=enriched,
        status="ok",
        error=None,
        error_traceback=None,
    )


# ── Main ───────────────────────────────────────────────────────────────────────


def main() -> None:
    """Entry point: parse args, run enrichment pipeline, emit summary."""
    parser = argparse.ArgumentParser(
        description="Sibling-based JSON enrichment for drug_master_linkage_unique",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python enrich_sibling_fill.py --mode=dry-run\n"
            "  python enrich_sibling_fill.py --mode=full-run --batch-size=1000\n"
        ),
    )
    parser.add_argument(
        "--mode",
        choices=["dry-run", "full-run"],
        required=True,
        help=(
            "dry-run  → compute enrichment & write audit rows only, "
            "no changes to drug_master_linkage_unique. "
            "full-run → same + write unified_json_enriched column."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        metavar="N",
        help="Rows fetched and processed per batch (default: 500).",
    )
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    run_mode_db = "dry_run" if args.mode == "dry-run" else "full_run"
    is_dry_run = args.mode == "dry-run"

    # Mirror all console output (including tqdm) to the project logs folder
    _console_log_path = LOG_DIR / f"run_{run_id}_console.log"
    _console_log_file = open(_console_log_path, "a", encoding="utf-8")  # noqa: SIM115
    sys.stdout = _TeeWriter(sys.__stdout__, _console_log_file)
    sys.stderr = _TeeWriter(sys.__stderr__, _console_log_file)

    summary_log, detailed_log, error_log, sections_log = setup_run_loggers(run_id)
    console = _console_logger()

    console.info("=" * 68)
    console.info("  Sibling Enrichment Pass")
    console.info("  run_id     : %s", run_id)
    console.info("  mode       : %s", args.mode)
    console.info("  batch_size : %d", args.batch_size)
    console.info("  log_dir    : %s", LOG_DIR)
    console.info("=" * 68)

    start_time = time.time()

    # ── Connect ────────────────────────────────────────────────────────────────
    try:
        conn = get_connection()
        conn.autocommit = False
    except Exception as exc:
        console.error("Failed to connect to database: %s", exc)
        sys.exit(1)

    # ── Dry-run reminder ───────────────────────────────────────────────────────
    if not is_dry_run:
        try:
            if not check_prior_dry_run(conn):
                console.warning(
                    "⚠  WARNING: No prior dry-run found in the audit table. "
                    "It is strongly recommended to run --mode=dry-run first to "
                    "verify enrichment before writing to drug_master_linkage_unique."
                )
        except Exception:
            pass  # audit table may not exist yet; setup_schema.sql wasn't run

    # ── Total row count ────────────────────────────────────────────────────────
    total = count_target_rows(conn)
    console.info("Target rows to process: %d", total)

    stats = RunStats()

    # ── Batch loop ─────────────────────────────────────────────────────────────
    with tqdm(
        total=total,
        desc=f"Enriching ({args.mode})",
        unit="rec",
        file=sys.stdout,
        dynamic_ncols=True,
    ) as pbar:
        for offset in range(0, total, args.batch_size):
            batch = fetch_batch(conn, offset, args.batch_size)
            if not batch:
                break

            # Collect unique (gf, df) pairs for this batch — one round-trip to DB
            pairs: list[tuple[str, str]] = list(
                {
                    (r["generic_formulation"], r["dosage_forms"])
                    for r in batch
                    if r.get("generic_formulation") and r.get("dosage_forms")
                }
            )
            try:
                siblings_map = fetch_siblings_for_pairs(conn, pairs)
            except Exception as exc:
                console.error(
                    "Failed to fetch siblings for batch at offset %d: %s", offset, exc
                )
                conn.rollback()
                raise

            batch_audit: list[AuditRow] = []

            for row in batch:
                target_mlid = row.get("master_linkage_id", "unknown")
                try:
                    result = process_record(row, siblings_map, run_id, run_mode_db)

                    # ── Accumulate stats ───────────────────────────────────────
                    if result.status == "ok":
                        stats.total_processed += 1
                        if result.fields_filled:
                            stats.total_enriched += 1
                            stats.total_fields_filled += len(result.fields_filled)
                            for fp in result.fields_filled:
                                stats.field_fill_counts[fp] = (
                                    stats.field_fill_counts.get(fp, 0) + 1
                                )
                        for ar in result.audit_rows:
                            if ar.status == "skipped_no_sibling_value":
                                stats.field_skip_counts[ar.field_path] = (
                                    stats.field_skip_counts.get(ar.field_path, 0) + 1
                                )
                        if result.fields_still_null:
                            stats.total_remaining_null_records += 1

                    batch_audit.extend(result.audit_rows)

                    # ── Detailed log entry (one JSON line per record) ──────────
                    detailed_log.info(
                        json.dumps(
                            {
                                "target_mlid": result.target_mlid,
                                "generic_formulation": result.generic_formulation,
                                "dosage_form": result.dosage_form,
                                "status": result.status,
                                "sibling_count": result.sibling_count,
                                "fields_filled_count": len(result.fields_filled),
                                "fields_filled": result.fields_filled,
                                "fields_still_null_count": len(
                                    result.fields_still_null
                                ),
                                "fields_still_null": result.fields_still_null,
                            },
                            ensure_ascii=False,
                        )
                    )

                except Exception as exc:
                    stats.total_errors += 1
                    tb = traceback.format_exc()

                    # ── Error log entry ────────────────────────────────────────
                    error_log.error(
                        json.dumps(
                            {
                                "target_mlid": target_mlid,
                                "exception_type": type(exc).__name__,
                                "exception_message": str(exc),
                                "traceback": tb,
                            },
                            ensure_ascii=False,
                        )
                    )
                    console.warning(
                        "Record error (target_mlid=%s): %s: %s",
                        target_mlid,
                        type(exc).__name__,
                        exc,
                    )

                    # Error audit sentinel row for the whole record
                    batch_audit.append(
                        AuditRow(
                            run_id=run_id,
                            run_mode=run_mode_db,
                            target_mlid=target_mlid,
                            generic_formulation=row.get("generic_formulation") or "",
                            dosage_form=row.get("dosage_forms") or "",
                            field_path="__record__",
                            original_value=None,
                            filled_value=None,
                            original_value_length=None,
                            filled_value_length=None,
                            source_sibling_mlid="",
                            sibling_count=0,
                            status="error",
                            error_message=f"{type(exc).__name__}: {exc}",
                        )
                    )

            # ── Bulk DB writes for this batch (audit only) ────────────────────
            try:
                bulk_insert_audit(conn, batch_audit)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                console.error(
                    "Batch commit failed at offset %d: %s", offset, exc
                )
                raise

            pbar.update(len(batch))
            pbar.set_postfix(
                enriched=stats.total_enriched,
                fields=stats.total_fields_filled,
                errors=stats.total_errors,
            )

    # ── Single server-side UPDATE (full-run only) ─────────────────────────────
    rows_updated = 0
    if not is_dry_run:
        console.info("Applying server-side UPDATE via apply_json_fills …")
        try:
            rows_updated = server_side_update_enriched_json(conn, run_id)
            conn.commit()
            console.info("unified_json_enriched updated for %d rows.", rows_updated)
        except Exception as exc:
            conn.rollback()
            console.error("Server-side UPDATE failed: %s", exc)
            raise

    # ── End-of-run summary ─────────────────────────────────────────────────────
    end_time = time.time()
    elapsed = end_time - start_time

    top_fields = sorted(
        stats.field_fill_counts.items(), key=lambda x: -x[1]
    )[:10]

    summary_data = {
        "run_id": run_id,
        "mode": args.mode,
        "start_time": datetime.fromtimestamp(start_time, tz=timezone.utc).isoformat(),
        "end_time": datetime.fromtimestamp(end_time, tz=timezone.utc).isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "total_processed": stats.total_processed,
        "total_enriched": stats.total_enriched,
        "total_fields_filled": stats.total_fields_filled,
        "total_errors": stats.total_errors,
        "records_with_remaining_nulls": stats.total_remaining_null_records,
        "top_10_fields_filled": [
            {"field": k, "count": v} for k, v in top_fields
        ],
    }
    summary_log.info(json.dumps(summary_data, ensure_ascii=False))

    # ── Console summary ────────────────────────────────────────────────────────
    bar = "=" * 68
    print(f"\n{bar}")
    print(f"  RUN SUMMARY  |  {args.mode.upper()}  |  {run_id[:8]}…")
    print(bar)
    print(f"  Total processed             : {stats.total_processed:>10,}")
    print(f"  Records enriched (≥1 field) : {stats.total_enriched:>10,}")
    print(f"  Total fields filled         : {stats.total_fields_filled:>10,}")
    print(f"  Records with remaining nulls: {stats.total_remaining_null_records:>10,}")
    print(f"  Errors                      : {stats.total_errors:>10,}")
    print(f"  Elapsed                     : {elapsed:>9.1f}s")
    if top_fields:
        print(f"\n  Top 10 filled fields:")
        for fp, cnt in top_fields:
            print(f"    {cnt:6,}  {fp}")
    print(bar)
    if is_dry_run:
        print(
            "\n  DRY-RUN: no changes written to drug_master_linkage_unique.\n"
            "  Review the audit table and logs, then re-run with --mode=full-run.\n"
        )
    else:
        print(
            f"\n  FULL-RUN: unified_json_enriched updated for "
            f"{stats.total_enriched:,} rows.\n"
        )

    # ── Sections log — one line per unique section path ───────────────────────
    # Section = first 3 parts of field path (e.g. dailymed.safety.precautions)
    # or the full path when depth < 3.
    def _section_key(fp: str) -> str:
        return ".".join(fp.split(".")[:3])

    all_sections: set[str] = set(
        _section_key(fp) for fp in stats.field_fill_counts
    ) | set(
        _section_key(fp) for fp in stats.field_skip_counts
    )

    for sec in sorted(all_sections):
        filled = sum(
            cnt for fp, cnt in stats.field_fill_counts.items()
            if _section_key(fp) == sec
        )
        not_filled = sum(
            cnt for fp, cnt in stats.field_skip_counts.items()
            if _section_key(fp) == sec
        )
        filled_fields = sorted(
            fp for fp in stats.field_fill_counts if _section_key(fp) == sec
        )
        not_filled_fields = sorted(
            fp for fp in stats.field_skip_counts if _section_key(fp) == sec
        )
        sections_log.info(
            json.dumps(
                {
                    "section": sec,
                    "filled_count": filled,
                    "not_filled_count": not_filled,
                    "filled_fields": filled_fields,
                    "not_filled_fields": not_filled_fields,
                },
                ensure_ascii=False,
            )
        )

    conn.close()
    console.info("Run complete. run_id=%s  elapsed=%.1fs", run_id, elapsed)
    console.info("Console log saved to: %s", _console_log_path)

    # Restore original stdout/stderr and close the console log file
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    _console_log_file.close()


if __name__ == "__main__":
    main()
