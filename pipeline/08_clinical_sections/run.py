#!/usr/bin/env python3
"""
populate_clinical_section.py

Extracts narrative label sections from drug label JSON data stored in
public.DrugMasterLinkage (combined_clean_jsonb) and populates the
drugdb.clinical_section PostgreSQL table.

Phase 1 — Test mode : preview 2 records, no inserts, confirm before proceeding.
Phase 2 — Full run  : stream all records, batch-insert with ON CONFLICT DO NOTHING.
"""

import json
import logging
import os
import sys
import traceback
from collections import defaultdict
from datetime import datetime

import psycopg2
import psycopg2.extras

# ─── Configuration ────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": 5432,
    "user": "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
    "dbname": "postgres",
}

LOG_FILE = "clinical_section_population.log"
BATCH_SIZE = 5000       # DrugMasterLinkage records per streaming batch
INSERT_PAGE = 500       # rows per execute_values page
TEST_LIMIT = 2

# Parent keys traversed under both openfda and dailymed
PARENT_KEYS = [
    "safety",
    "labeling_content",
    "clinical",
    "adverse_events",
    "drug_interactions",
    "patient_info",
    "supply_storage",
    "abuse_dependence",
    "population_specific",
]

SEP = "═" * 72

# ─── Table setup ──────────────────────────────────────────────────────────────

def ensure_table(conn, log: logging.Logger) -> None:
    """Create drugdb.clinical_section (and its indexes) if they don't exist."""
    log.info("Ensuring drugdb.clinical_section table exists…")
    ddl = """
        CREATE SCHEMA IF NOT EXISTS drugdb;

        CREATE TABLE IF NOT EXISTS drugdb.clinical_section (
            id                 SERIAL PRIMARY KEY,
            formulation_id     UUID NOT NULL
                               REFERENCES drugdb.drug(formulation_id)
                               ON DELETE CASCADE,
            section            TEXT NOT NULL,
            text               TEXT,
            subsections        JSONB DEFAULT '[]',
            source             TEXT,
            source_document_id TEXT,
            UNIQUE(formulation_id, section, source)
        );

        CREATE INDEX IF NOT EXISTS idx_cs_formulation
            ON drugdb.clinical_section(formulation_id);

        CREATE INDEX IF NOT EXISTS idx_cs_section
            ON drugdb.clinical_section(section);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Table drugdb.clinical_section is ready.")
    print("Table drugdb.clinical_section is ready.\n")


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("clinical_section")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ─── Extraction helpers ───────────────────────────────────────────────────────

def _clean_text(val) -> str | None:
    """Return stripped text or None when empty/whitespace."""
    if not val:
        return None
    s = str(val).strip()
    return s if s else None


def extract_openfda_sections(jsonb_data: dict, log: logging.Logger, mid) -> tuple[list, int]:
    """
    Traverse openfda parent keys and return a list of section dicts plus the
    count of sections skipped due to empty text (no subsections in OpenFDA).
    """
    sections: list[dict] = []
    skipped = 0

    openfda = jsonb_data.get("openfda")
    if not openfda or not isinstance(openfda, dict):
        return sections, skipped

    # source_document_id = openfda.identification.set_id
    doc_id: str | None = None
    ident = openfda.get("identification")
    if isinstance(ident, dict):
        doc_id = ident.get("set_id") or None
    if not doc_id:
        log.warning(f"[{mid}] openfda.identification.set_id missing")

    for pk in PARENT_KEYS:
        parent = openfda.get(pk)
        if parent is None:
            log.debug(f"[{mid}] openfda: parent key '{pk}' absent")
            continue
        if not isinstance(parent, dict):
            log.warning(f"[{mid}] openfda: parent key '{pk}' is not a dict — skipped")
            continue

        for section_key, section_data in parent.items():
            if not isinstance(section_data, dict):
                continue

            text = _clean_text(section_data.get("text"))

            # OpenFDA has no subsections — skip when text is also empty
            if text is None:
                log.debug(
                    f"[{mid}] openfda: skipping '{section_key}' — empty text, no subsections"
                )
                skipped += 1
                continue

            sections.append(
                {
                    "section": section_key,
                    "text": text,
                    "subsections": [],       # OpenFDA never has subsections
                    "source": "openfda",
                    "source_document_id": doc_id,
                }
            )

    return sections, skipped


def extract_dailymed_sections(jsonb_data: dict, log: logging.Logger, mid) -> tuple[list, int]:
    """
    Traverse dailymed parent keys and return a list of section dicts plus the
    count of sections skipped due to empty content AND empty subsections.
    DailyMed uses 'content' (not 'text') and has a 'subsections' array.
    """
    sections: list[dict] = []
    skipped = 0

    dailymed = jsonb_data.get("dailymed")
    if not dailymed or not isinstance(dailymed, dict):
        return sections, skipped

    # source_document_id = dailymed.identification.drug_label.document_id
    doc_id: str | None = None
    ident = dailymed.get("identification")
    if isinstance(ident, dict):
        drug_label = ident.get("drug_label")
        if isinstance(drug_label, dict):
            doc_id = drug_label.get("document_id") or None
    if not doc_id:
        log.warning(f"[{mid}] dailymed.identification.drug_label.document_id missing")

    for pk in PARENT_KEYS:
        parent = dailymed.get(pk)
        if parent is None:
            log.debug(f"[{mid}] dailymed: parent key '{pk}' absent")
            continue
        if not isinstance(parent, dict):
            log.warning(f"[{mid}] dailymed: parent key '{pk}' is not a dict — skipped")
            continue

        for section_key, section_data in parent.items():
            if not isinstance(section_data, dict):
                continue

            text = _clean_text(section_data.get("content"))

            # Transform subsections to standard shape
            raw_subs = section_data.get("subsections") or []
            subsections: list[dict] = []
            if isinstance(raw_subs, list):
                for i, sub in enumerate(raw_subs):
                    if not isinstance(sub, dict):
                        continue
                    subsections.append(
                        {
                            "subsection_id": f"{section_key}_{i}",
                            "title": sub.get("section_title") or "",
                            "text": sub.get("content") or "",
                        }
                    )

            # Skip only when BOTH text is empty AND no subsections
            if text is None and not subsections:
                log.debug(
                    f"[{mid}] dailymed: skipping '{section_key}' — empty content, no subsections"
                )
                skipped += 1
                continue

            sections.append(
                {
                    "section": section_key,
                    "text": text,
                    "subsections": subsections,
                    "source": "dailymed",
                    "source_document_id": doc_id,
                }
            )

    return sections, skipped


# ─── Database helpers ─────────────────────────────────────────────────────────

def get_formulation_ids(cur, master_linkage_id) -> list:
    """Return all formulation_ids (UUID) from drugdb.drug for a master_linkage_id."""
    cur.execute(
        "SELECT formulation_id FROM drugdb.drug WHERE master_linkage_id = %s",
        (master_linkage_id,),
    )
    return [row[0] for row in cur.fetchall()]


INSERT_SQL = """
    INSERT INTO drugdb.clinical_section
        (formulation_id, section, text, subsections, source, source_document_id)
    VALUES %s
    ON CONFLICT (formulation_id, section, source) DO NOTHING
    RETURNING id
"""


def batch_insert(cur, rows: list) -> int:
    """
    Insert rows with execute_values (ON CONFLICT DO NOTHING) and return the
    number of rows actually inserted (RETURNING id count).
    Returns 0 when rows is empty.
    """
    if not rows:
        return 0
    result = psycopg2.extras.execute_values(
        cur, INSERT_SQL, rows, page_size=INSERT_PAGE, fetch=True
    )
    return len(result)


# ─── Phase 1 — Test mode ──────────────────────────────────────────────────────

def run_test_mode(conn, log: logging.Logger) -> None:
    print(f"\n{SEP}")
    print("PHASE 1 — TEST MODE  (2 records · no inserts)")
    print(SEP + "\n")
    log.info("Phase 1 — test mode starting")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            'SELECT master_linkage_id, combined_clean_jsonb '
            'FROM public."DrugMasterLinkage" '
            'LIMIT %s',
            (TEST_LIMIT,),
        )
        records = cur.fetchall()

    if not records:
        print("No records found in public.DrugMasterLinkage — nothing to process.")
        log.warning("No records found in public.DrugMasterLinkage")
        return

    with conn.cursor() as cur:
        for rec in records:
            mid = rec["master_linkage_id"]
            jdata = rec["combined_clean_jsonb"]

            # Best-effort drug name from openfda.drug_info.generic_name
            try:
                drug_name = (
                    (jdata.get("openfda") or {})
                    .get("drug_info") or {}
                ).get("generic_name") or "Unknown"
            except Exception:
                drug_name = "Unknown"

            fids = get_formulation_ids(cur, mid)

            of_secs, of_skip = extract_openfda_sections(jdata, log, mid)
            dm_secs, dm_skip = extract_dailymed_sections(jdata, log, mid)
            all_secs = of_secs + dm_secs

            proj = len(fids) * len(all_secs)

            print(f"Drug               : {drug_name}")
            print(f"master_linkage_id  : {mid}")
            print(f"Formulation IDs    : {len(fids)} found")
            print(f"OpenFDA  sections  : {len(of_secs)}  (skipped {of_skip})")
            print(f"DailyMed sections  : {len(dm_secs)}  (skipped {dm_skip})")
            print(
                f"Projected rows     : {len(fids)} formulations × "
                f"{len(all_secs)} sections = {proj} rows"
            )

            if all_secs:
                hdr = f"\n  {'Source':<10} {'Section':<48} {'TxtLen':>6} {'Subs':>4}  DocID"
                print(hdr)
                print(f"  {'-'*10} {'-'*48} {'-'*6} {'-'*4}  {'-'*24}")
                for s in all_secs:
                    tl = len(s["text"]) if s["text"] else 0
                    sc = len(s["subsections"])
                    did = (s["source_document_id"] or "—")[:24]
                    print(
                        f"  {s['source']:<10} {s['section']:<48} "
                        f"{tl:>6} {sc:>4}  {did}"
                    )
            print()

    print(SEP)
    print("Test mode complete — review output above.")
    print(SEP)
    log.info("Phase 1 — test mode complete")


# ─── Phase 2 — Full run ───────────────────────────────────────────────────────

def run_full_mode(conn_read, conn_write, log: logging.Logger) -> None:
    log.info("Phase 2 — full run starting")
    t_start = datetime.now()

    # Counters
    total_records = 0
    total_fids = 0
    total_attempted = 0
    total_inserted = 0
    total_conflict = 0
    total_sec_skipped = 0
    total_warn_no_fid = 0
    source_inserted: dict[str, int] = defaultdict(int)
    section_drug_count: dict[str, int] = defaultdict(int)

    # conn_read uses a server-side named cursor so it never loads all rows into memory.
    # conn_write handles all formulation lookups and inserts; commits independently.
    with conn_read.cursor(
        "linkage_stream", cursor_factory=psycopg2.extras.RealDictCursor
    ) as stream:
        stream.itersize = BATCH_SIZE
        stream.execute(
            'SELECT master_linkage_id, combined_clean_jsonb '
            'FROM public."DrugMasterLinkage"'
        )

        batch_num = 0
        while True:
            rows = stream.fetchmany(BATCH_SIZE)
            if not rows:
                break

            batch_num += 1
            b_start = datetime.now()
            b_attempted = 0
            b_inserted = 0

            log.info(f"Batch {batch_num}: received {len(rows)} records")

            for rec in rows:
                mid = rec["master_linkage_id"]
                jdata = rec["combined_clean_jsonb"]
                total_records += 1

                try:
                    # Formulation IDs for this master linkage
                    with conn_write.cursor() as wc:
                        fids = get_formulation_ids(wc, mid)

                    if not fids:
                        log.warning(
                            f"No formulation_id found for master_linkage_id={mid} — skipped"
                        )
                        total_warn_no_fid += 1
                        continue

                    total_fids += len(fids)

                    of_secs, of_skip = extract_openfda_sections(jdata, log, mid)
                    dm_secs, dm_skip = extract_dailymed_sections(jdata, log, mid)
                    total_sec_skipped += of_skip + dm_skip

                    if not of_secs and not dm_secs:
                        continue

                    # Build insert tuples per source for accurate per-source counting
                    openfda_rows = []
                    dailymed_rows = []
                    for fid in fids:
                        for s in of_secs:
                            openfda_rows.append((
                                fid,
                                s["section"],
                                s["text"],
                                json.dumps(s["subsections"]),
                                "openfda",
                                s["source_document_id"],
                            ))
                        for s in dm_secs:
                            dailymed_rows.append((
                                fid,
                                s["section"],
                                s["text"],
                                json.dumps(s["subsections"]),
                                "dailymed",
                                s["source_document_id"],
                            ))

                    attempted = len(openfda_rows) + len(dailymed_rows)
                    b_attempted += attempted
                    total_attempted += attempted

                    with conn_write.cursor() as wc:
                        of_n = batch_insert(wc, openfda_rows)
                        dm_n = batch_insert(wc, dailymed_rows)

                    conn_write.commit()

                    n_inserted = of_n + dm_n
                    n_conflict = attempted - n_inserted

                    b_inserted += n_inserted
                    total_inserted += n_inserted
                    total_conflict += n_conflict
                    source_inserted["openfda"] += of_n
                    source_inserted["dailymed"] += dm_n

                    # Track sections per drug (count each section once per drug)
                    seen = set()
                    for s in of_secs + dm_secs:
                        key = s["section"]
                        if key not in seen:
                            section_drug_count[key] += 1
                            seen.add(key)

                except Exception:
                    log.error(
                        f"Error processing master_linkage_id={mid}:\n"
                        f"{traceback.format_exc()}"
                    )
                    conn_write.rollback()

            b_elapsed = (datetime.now() - b_start).total_seconds()
            log.info(
                f"Batch {batch_num} done — records={len(rows)}, "
                f"attempted={b_attempted}, inserted={b_inserted}, "
                f"elapsed={b_elapsed:.1f}s"
            )
            print(
                f"Batch {batch_num:>4} | records_processed={total_records:>8,} | "
                f"rows_inserted={total_inserted:>10,} | conflicts={total_conflict:>8,}"
            )

    elapsed = (datetime.now() - t_start).total_seconds()

    # ── Summary ──────────────────────────────────────────────────────────────
    lines = [
        "",
        SEP,
        "FINAL SUMMARY",
        SEP,
        f"DrugMasterLinkage records processed  : {total_records:>10,}",
        f"Formulation IDs found                : {total_fids:>10,}",
        f"Rows attempted (fid × section)       : {total_attempted:>10,}",
        f"Rows inserted                        : {total_inserted:>10,}",
        f"Rows skipped (ON CONFLICT)           : {total_conflict:>10,}",
        f"Sections skipped (empty text+subs)   : {total_sec_skipped:>10,}",
        f"Warnings — no formulation match      : {total_warn_no_fid:>10,}",
        "",
        "Breakdown by source (rows inserted):",
    ]
    for src in ("openfda", "dailymed"):
        cnt = source_inserted.get(src, 0)
        lines.append(f"  {src:<20}: {cnt:,}")

    lines.append("")
    lines.append("Breakdown by section (number of drugs with that section, descending):")
    for sec, cnt in sorted(section_drug_count.items(), key=lambda x: -x[1]):
        lines.append(f"  {sec:<60}: {cnt:>6,} drugs")

    lines += [
        "",
        f"Total elapsed    : {elapsed:,.1f}s  ({elapsed / 60:,.1f} min)",
        f"Log file         : {LOG_FILE}",
        SEP,
    ]

    for line in lines:
        log.info(line)
        print(line)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    log = setup_logging()
    log.info(
        f"=== populate_clinical_section.py started "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==="
    )

    print(f"\nLog → {LOG_FILE}")
    print("Connecting to database…")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        log.info("Database connection established")
        print("Connected.\n")
    except Exception as e:
        log.error(f"Connection failed: {e}")
        print(f"ERROR: Could not connect — {e}")
        sys.exit(1)

    try:
        ensure_table(conn, log)

        run_test_mode(conn, log)

        print("\nProceed with full run? (yes / no): ", end="", flush=True)
        answer = input().strip().lower()

        if answer not in ("yes", "y"):
            print("Full run cancelled.")
            log.info("Full run cancelled by user")
            return

        # Offer to truncate before starting
        print(
            "\nTable drugdb.clinical_section exists.\n"
            "Do you want to TRUNCATE it before starting the full run?\n"
            "(This deletes ALL existing rows and resets the id sequence.)\n"
            "Truncate? (yes / no): ",
            end="",
            flush=True,
        )
        trunc_answer = input().strip().lower()
        if trunc_answer in ("yes", "y"):
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE TABLE drugdb.clinical_section RESTART IDENTITY"
                )
            conn.commit()
            log.info("Table truncated — starting fresh.")
            print("Table truncated. Starting fresh.\n")
        else:
            log.info("Keeping existing rows — ON CONFLICT DO NOTHING is active.")
            print("Keeping existing rows.\n")

        # Open a dedicated write connection so the server-side read cursor
        # (which lives inside conn's open transaction) is never interrupted
        # by commits or rollbacks.
        print("Opening write connection…")
        conn_write = psycopg2.connect(**DB_CONFIG)
        conn_write.autocommit = False

        print(f"\n{SEP}")
        print("PHASE 2 — FULL RUN")
        print(SEP + "\n")
        log.info("User confirmed — Phase 2 starting")

        try:
            run_full_mode(conn, conn_write, log)
        finally:
            conn_write.close()

    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        log.warning("Script interrupted (KeyboardInterrupt)")

    finally:
        conn.close()
        log.info(
            f"=== Script ended {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==="
        )


if __name__ == "__main__":
    main()
