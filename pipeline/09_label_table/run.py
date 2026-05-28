#!/usr/bin/env python3
"""
populate_label_table.py

Extracts structured tables from drug label JSON (combined_clean_jsonb in
DrugMasterLinkage) and populates the label_table PostgreSQL table.

Phase 1 — Test: preview 2 records, no insert, ask for confirmation.
Phase 2 — Full: stream all records, insert with ON CONFLICT DO NOTHING.
"""

import json
import logging
import os
import re
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

# ─── Configuration ─────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": 5432,
    "user": "postgres",
    "dbname": "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
}

LOG_FILE = "label_table_population.log"
BATCH_SIZE = 5000           # rows in pending list before flush
STREAM_FETCH_SIZE = 200     # rows fetched at a time from server-side cursor
TEST_LIMIT = 2              # DrugMasterLinkage records in test mode

# Parent keys to check under openfda
OPENFDA_PARENT_KEYS = [
    "safety",
    "adverse_events",
    "labeling_content",
    "clinical",
    "drug_interactions",
    "population_specific",
]

# Section key → semantic_type
SEMANTIC_TYPE_MAP: Dict[str, str] = {
    "warnings_and_cautions": "adverse_event",
    "adverse_reactions": "adverse_event",
    "dosage_and_administration": "dosing",
    "drug_interactions": "interaction",
    "pharmacokinetics": "pharmacokinetics",
    "clinical_pharmacology": "pharmacokinetics",
    "clinical_studies": "clinical_study",
    "contraindications": "contraindication",
}


# ─── Logging ───────────────────────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("label_table")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ─── Extraction helpers ────────────────────────────────────────────────────
_HEADER_PAT = re.compile(r"^[n%]+$", re.IGNORECASE)
_TABLE_NUM_PAT = re.compile(r"\bTable\s+(\d+)\b", re.IGNORECASE)


def _is_header_row(row: Any) -> bool:
    """True if every non-empty cell looks like n / % (unit-only header row)."""
    if isinstance(row, list):
        vals = [str(v).strip() for v in row if v is not None and str(v).strip()]
    elif isinstance(row, dict):
        vals = [str(v).strip() for v in row.values() if v is not None and str(v).strip()]
    else:
        return False
    return bool(vals) and all(_HEADER_PAT.match(v) for v in vals)


def _row_to_strings(row: Any) -> List[str]:
    if isinstance(row, list):
        return [str(v) for v in row]
    if isinstance(row, dict):
        return [str(v) for v in row.values()]
    return []


def _caption_table_num(caption: Optional[str]) -> Optional[int]:
    m = _TABLE_NUM_PAT.search(caption or "")
    return int(m.group(1)) if m else None


def _build_table_record(
    formulation_id: str,
    section_key: str,
    tbl: Dict,
    idx: int,
) -> Optional[Dict]:
    """
    Build a single label_table row dict from a raw table object.
    Returns None if tbl is not a dict.
    """
    if not isinstance(tbl, dict):
        return None

    raw_caption = tbl.get("caption", "")
    caption: Optional[str] = raw_caption.strip() or None

    tbl_num = _caption_table_num(caption)
    tbl_idx = tbl_num if tbl_num is not None else idx
    table_id = f"{formulation_id}_{section_key}_table_{tbl_idx}"

    headers: List[str] = list(tbl.get("headers") or [])
    rows: List = list(tbl.get("rows") or [])

    # Promote first row to headers if it looks like n/% labels
    if not headers and rows and _is_header_row(rows[0]):
        headers = _row_to_strings(rows.pop(0))

    return {
        "formulation_id": formulation_id,
        "table_id": table_id,
        "caption": caption,
        "semantic_type": SEMANTIC_TYPE_MAP.get(section_key),
        "section": section_key,
        "headers": headers,
        "rows_data": rows,
    }


# ─── openfda extraction ────────────────────────────────────────────────────
def _extract_openfda_tables(
    openfda: Dict,
    formulation_id: str,
    logger: logging.Logger,
) -> List[Dict]:
    results: List[Dict] = []
    for parent_key in OPENFDA_PARENT_KEYS:
        parent = openfda.get(parent_key)
        if not isinstance(parent, dict):
            continue
        for section_key, section_data in parent.items():
            if not isinstance(section_data, dict):
                continue
            tables = section_data.get("table")
            if not tables or not isinstance(tables, list):
                continue
            for idx, tbl in enumerate(tables):
                rec = _build_table_record(formulation_id, section_key, tbl, idx)
                if rec is None:
                    logger.warning(
                        f"  openfda.{parent_key}.{section_key}[{idx}]: "
                        f"table entry is not a dict — skipped"
                    )
                    continue
                results.append(rec)
    return results


# ─── dailymed extraction ───────────────────────────────────────────────────
def _extract_dailymed_tables(
    node: Any,
    formulation_id: str,
    logger: logging.Logger,
    path: str = "dailymed",
) -> List[Dict]:
    """
    Recursively find explicit table[] arrays inside the dailymed structure.
    Skips text / content leaves entirely — only acts on 'table' keys.
    """
    results: List[Dict] = []
    if not isinstance(node, dict):
        return results

    raw_tables = node.get("table")
    if isinstance(raw_tables, list) and raw_tables:
        section_key = path.split(".")[-1]
        for idx, tbl in enumerate(raw_tables):
            rec = _build_table_record(formulation_id, section_key, tbl, idx)
            if rec is None:
                continue
            # Override section with full dotted path for traceability
            rec["section"] = path
            rec["semantic_type"] = SEMANTIC_TYPE_MAP.get(section_key)
            results.append(rec)
    else:
        skip = {"table", "content", "text"}
        for key, val in node.items():
            if key in skip:
                continue
            child_path = f"{path}.{key}"
            if isinstance(val, dict):
                results.extend(
                    _extract_dailymed_tables(val, formulation_id, logger, child_path)
                )
            elif isinstance(val, list):
                for item in val:
                    results.extend(
                        _extract_dailymed_tables(item, formulation_id, logger, child_path)
                    )

    return results


# ─── Public extraction entry point ─────────────────────────────────────────
def extract_tables(
    data: Dict,
    formulation_id: str,
    logger: logging.Logger,
) -> List[Dict]:
    """Extract all tables from one drug's combined_clean_jsonb."""
    results: List[Dict] = []

    openfda = data.get("openfda")
    if isinstance(openfda, dict):
        results.extend(_extract_openfda_tables(openfda, formulation_id, logger))

    dailymed = data.get("dailymed")
    if isinstance(dailymed, dict):
        results.extend(_extract_dailymed_tables(dailymed, formulation_id, logger))

    return results


# ─── DB helpers ────────────────────────────────────────────────────────────
def get_formulation_ids(cur, master_linkage_id: str) -> List[str]:
    cur.execute(
        "SELECT formulation_id FROM drugdb.drug WHERE master_linkage_id = %s::uuid",
        (master_linkage_id,),
    )
    return [str(r[0]) for r in cur.fetchall()]


def ensure_schema_and_table(conn, logger: logging.Logger) -> None:
    """Create drugdb.label_table and its unique index if they don't exist."""
    with conn.cursor() as cur:
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS drugdb.label_table (
                    id             SERIAL PRIMARY KEY,
                    formulation_id UUID NOT NULL,
                    table_id       TEXT NOT NULL,
                    caption        TEXT,
                    semantic_type  TEXT,
                    section        TEXT,
                    headers        TEXT[],
                    rows_data      JSONB DEFAULT '[]'
                )
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uidx_label_table_fid_tid
                ON drugdb.label_table (formulation_id, table_id)
            """)
            conn.commit()
            logger.info("drugdb.label_table created and unique index ensured")
        except Exception as exc:
            conn.rollback()
            logger.warning(f"Schema/table setup warning: {exc}")


# ─── Phase 1: Test mode ─────────────────────────────────────────────────────
def run_test_mode(logger: logging.Logger) -> bool:
    logger.info("=" * 62)
    logger.info("PHASE 1 — TEST MODE  (2 records, no insert)")
    logger.info("=" * 62)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT master_linkage_id, combined_clean_jsonb
                FROM public."DrugMasterLinkage"
                LIMIT %s
                """,
                (TEST_LIMIT,),
            )
            linkage_rows = cur.fetchall()

        print(f"\n{'═'*62}")
        print(f"  TEST MODE — {len(linkage_rows)} DrugMasterLinkage records")
        print(f"{'═'*62}\n")

        total_sections = 0
        total_tables = 0

        for i, row in enumerate(linkage_rows, 1):
            mlid = str(row["master_linkage_id"])
            data = row["combined_clean_jsonb"]
            if isinstance(data, str):
                data = json.loads(data)

            with conn.cursor() as cur2:
                fids = get_formulation_ids(cur2, mlid)

            # Drug name
            drug_name = "(unknown)"
            if fids:
                with conn.cursor() as cur3:
                    cur3.execute(
                        "SELECT generic_name FROM drugdb.drug WHERE formulation_id = %s LIMIT 1",
                        (fids[0],),
                    )
                    r = cur3.fetchone()
                    if r:
                        drug_name = r[0]

            # Count openfda child sections
            openfda = data.get("openfda") or {}
            sections_checked = sum(
                len(openfda.get(pk) or {})
                for pk in OPENFDA_PARENT_KEYS
                if isinstance(openfda.get(pk), dict)
            )

            # Extract using the first formulation_id for preview
            sample_fid = fids[0] if fids else mlid
            sample_tables = extract_tables(data, sample_fid, logger)
            rows_if_all_fids = len(sample_tables) * max(len(fids), 1)

            total_sections += sections_checked
            total_tables += rows_if_all_fids

            print(f"  Record {i}  ─  {drug_name}")
            print(f"    master_linkage_id : {mlid}")
            print(f"    formulation_ids   : {len(fids)} found  →  {fids[:4]}"
                  + ("  ..." if len(fids) > 4 else ""))
            print(f"    Sections checked  : {sections_checked}")
            print(f"    Unique tables     : {len(sample_tables)}")
            print(f"    Rows to insert    : {len(sample_tables)} tables × "
                  f"{len(fids)} formulations = {rows_if_all_fids}")
            print()

            for j, tbl in enumerate(sample_tables):
                print(f"    {'┌' if j == 0 else '├'}─ section       : {tbl['section']}")
                print(f"    │  table_id      : {tbl['table_id']}")
                print(f"    │  caption       : {tbl['caption']}")
                print(f"    │  semantic_type : {tbl['semantic_type']}")
                print(f"    │  headers ({len(tbl['headers'])})   : "
                      f"{tbl['headers'][:6]}{'...' if len(tbl['headers']) > 6 else ''}")
                print(f"    {'└' if j == len(sample_tables)-1 else '│'}  row count     : "
                      f"{len(tbl['rows_data'])}")
                if j < len(sample_tables) - 1:
                    print(f"    │")
                if j >= 29:
                    remaining = len(sample_tables) - 30
                    if remaining > 0:
                        print(f"    └─ ... {remaining} more tables (truncated)")
                    break
            print()

            logger.info(
                f"TEST record {i}: drug={drug_name!r}  mlid={mlid}  "
                f"fids={len(fids)}  sections={sections_checked}  "
                f"unique_tables={len(sample_tables)}  rows_to_insert={rows_if_all_fids}"
            )

        print(f"{'─'*62}")
        print(f"  Total sections checked : {total_sections}")
        print(f"  Total rows to insert   : {total_tables} (across both records)")
        print(f"{'─'*62}")

    finally:
        conn.close()

    answer = input("\nProceed with full run? [y/N]: ").strip().lower()
    return answer == "y"


# ─── Phase 2: Full run ──────────────────────────────────────────────────────
_INSERT_SQL = """
    INSERT INTO drugdb.label_table
        (formulation_id, table_id, caption, semantic_type, section, headers, rows_data)
    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (formulation_id, table_id) DO NOTHING
"""


def _flush(
    cur,
    conn,
    pending: List,
    batch_num: int,
    records_so_far: int,
    logger: logging.Logger,
    label: str = "",
) -> Tuple[int, int]:
    """Execute batch insert, commit, return (new_batch_num, rows_inserted)."""
    batch_num += 1
    tag = label or str(batch_num)
    t0 = datetime.now()
    try:
        psycopg2.extras.execute_batch(cur, _INSERT_SQL, pending, page_size=1000)
        conn.commit()
        n = len(pending)
        elapsed = (datetime.now() - t0).total_seconds()
        msg = (
            f"Batch {tag}: {n:,} rows inserted  "
            f"({records_so_far:,} linkage records so far)  [{elapsed:.1f}s]"
        )
        logger.info(msg)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        return batch_num, n
    except Exception as exc:
        conn.rollback()
        logger.error(
            f"Batch {tag} insert FAILED: {exc}\n{traceback.format_exc()}"
        )
        return batch_num, 0


def run_full(logger: logging.Logger, record_limit: int = 0) -> None:
    """
    record_limit > 0  → process only that many DrugMasterLinkage records (test-insert mode).
    record_limit = 0  → process all records (full run).
    """
    label = f"FULL RUN" if record_limit == 0 else f"TEST-INSERT (limit {record_limit})"
    logger.info("=" * 62)
    logger.info(f"PHASE 2 — {label} STARTED")
    logger.info("=" * 62)

    start_time = datetime.now()

    # Two connections: read (streaming) + write (inserts + lookups)
    read_conn = psycopg2.connect(**DB_CONFIG)
    read_conn.autocommit = False      # named cursor needs an open transaction

    write_conn = psycopg2.connect(**DB_CONFIG)
    write_conn.autocommit = False

    ensure_schema_and_table(write_conn, logger)

    stats: Dict[str, Any] = {
        "total_linkage_records": 0,
        "total_formulation_ids": 0,
        "total_rows_inserted": 0,
        "total_skipped_no_formulation": 0,
        "by_semantic_type": {},
        "by_section": {},
    }

    pending: List = []
    batch_num = 0

    try:
        # Server-side cursor streams DrugMasterLinkage without loading all rows
        with read_conn.cursor(
            "stream_linkage",
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as stream:
            stream.itersize = STREAM_FETCH_SIZE
            stream.execute(
                'SELECT master_linkage_id, combined_clean_jsonb '
                'FROM public."DrugMasterLinkage"'
            )

            lookup_cur = write_conn.cursor()
            insert_cur = write_conn.cursor()

            for linkage_row in stream:
                if record_limit and stats["total_linkage_records"] >= record_limit:
                    break
                stats["total_linkage_records"] += 1
                mlid = str(linkage_row["master_linkage_id"])

                # ── Formulation ID lookup ──────────────────────────────
                try:
                    fids = get_formulation_ids(lookup_cur, mlid)
                except Exception as exc:
                    logger.error(f"Lookup error mlid={mlid}: {exc}")
                    continue

                if not fids:
                    logger.warning(
                        f"No formulation_ids for master_linkage_id={mlid} — skipped"
                    )
                    stats["total_skipped_no_formulation"] += 1
                    continue

                stats["total_formulation_ids"] += len(fids)

                # ── Parse JSON ────────────────────────────────────────
                data = linkage_row["combined_clean_jsonb"]
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError as exc:
                        logger.error(f"JSON parse error mlid={mlid}: {exc}")
                        continue

                # ── Extract and queue ─────────────────────────────────
                for fid in fids:
                    try:
                        tables = extract_tables(data, fid, logger)
                    except Exception as exc:
                        logger.error(
                            f"Extraction error fid={fid}: {exc}\n"
                            f"{traceback.format_exc()}"
                        )
                        continue

                    for tbl in tables:
                        pending.append((
                            tbl["formulation_id"],
                            tbl["table_id"],
                            tbl["caption"],
                            tbl["semantic_type"],
                            tbl["section"],
                            tbl["headers"],          # TEXT[] — list of str
                            psycopg2.extras.Json(tbl["rows_data"]),  # JSONB
                        ))
                        st = tbl["semantic_type"] or "NULL"
                        stats["by_semantic_type"][st] = (
                            stats["by_semantic_type"].get(st, 0) + 1
                        )
                        sec = tbl["section"]
                        stats["by_section"][sec] = (
                            stats["by_section"].get(sec, 0) + 1
                        )

                # ── Flush if batch full ───────────────────────────────
                if len(pending) >= BATCH_SIZE:
                    batch_num, n = _flush(
                        insert_cur, write_conn, pending,
                        batch_num, stats["total_linkage_records"], logger,
                    )
                    stats["total_rows_inserted"] += n
                    pending = []

            lookup_cur.close()

            # ── Final flush ───────────────────────────────────────────
            if pending:
                batch_num, n = _flush(
                    insert_cur, write_conn, pending,
                    batch_num, stats["total_linkage_records"], logger,
                    label="FINAL",
                )
                stats["total_rows_inserted"] += n

            insert_cur.close()

    finally:
        try:
            read_conn.close()
        except Exception:
            pass
        try:
            write_conn.close()
        except Exception:
            pass

    _print_summary(stats, start_time, logger)


def _print_summary(
    stats: Dict[str, Any],
    start_time: datetime,
    logger: logging.Logger,
) -> None:
    elapsed = (datetime.now() - start_time).total_seconds()
    lines = [
        "=" * 62,
        "FINAL SUMMARY",
        "=" * 62,
        f"  DrugMasterLinkage records processed  : {stats['total_linkage_records']:>10,}",
        f"  Formulation IDs found                : {stats['total_formulation_ids']:>10,}",
        f"  label_table rows inserted            : {stats['total_rows_inserted']:>10,}",
        f"  Skipped (no formulation match)       : {stats['total_skipped_no_formulation']:>10,}",
        "",
        "  Breakdown by semantic_type:",
    ]
    for k, v in sorted(stats["by_semantic_type"].items()):
        lines.append(f"    {k:<35}  {v:>8,}")
    lines += ["", "  Breakdown by section:"]
    for k, v in sorted(stats["by_section"].items()):
        lines.append(f"    {k:<45}  {v:>8,}")
    lines += [
        "",
        f"  Total time  : {elapsed:.1f}s  ({elapsed / 60:.1f} min)",
        f"  Log file    : {LOG_FILE}",
        "=" * 62,
    ]
    for line in lines:
        logger.info(f"[SUMMARY] {line}")
        print(line)


# ─── Entry point ───────────────────────────────────────────────────────────
def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Populate drugdb.label_table from DrugMasterLinkage")
    parser.add_argument(
        "--test-insert", metavar="N", type=int, nargs="?", const=5,
        help="Run Phase 2 on N records only (default 5) to verify inserts, then exit"
    )
    args = parser.parse_args()
    test_insert_limit: int = args.test_insert or 0

    logger = setup_logging()
    logger.info("─" * 62)
    logger.info("populate_label_table.py  started")
    if test_insert_limit:
        logger.info(f"Mode: --test-insert {test_insert_limit}")
    logger.info("─" * 62)

    # Phase 1 — preview (always runs)
    try:
        proceed = run_test_mode(logger)
    except Exception as exc:
        logger.error(f"Test mode error: {exc}\n{traceback.format_exc()}")
        sys.exit(1)

    if not proceed:
        logger.info("User chose not to proceed. No data inserted. Exiting.")
        print("\nAborted — no data was written to label_table.")
        logger.info("Script finished (aborted by user)")
        return

    # Phase 2 — full run (or limited test-insert run)
    try:
        run_full(logger, record_limit=test_insert_limit)
    except KeyboardInterrupt:
        logger.warning("Run interrupted by user (KeyboardInterrupt)")
        print("\nInterrupted.")
    except Exception as exc:
        logger.error(f"Full-run error: {exc}\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        logger.info("Script finished")


if __name__ == "__main__":
    main()
