#!/usr/bin/env python3
"""
Phase 3 Drug Indication Extraction Pipeline
============================================

What this script does:
    Extracts structured drug indications from FDA label text using a vLLM
    server (Qwen2.5-72B-Instruct-AWQ). Streams drugs from PostgreSQL 500
    rows at a time, sends prompts concurrently to vLLM (CONCURRENCY=32),
    and flushes results to DB every 500 drugs so nothing is lost on crash.

How to run:
    Step 1 — Start vLLM on your RunPod A100:

        python -m vllm.entrypoints.openai.api_server \
            --model Qwen/Qwen2.5-72B-Instruct-AWQ \
            --quantization awq_marlin \
            --max-model-len 4096 \
            --gpu-memory-utilization 0.90 \
            --max-num-seqs 64 \
            --enable-prefix-caching \
            --host 0.0.0.0 \
            --port 8000

    Step 2 — Set environment variables and run:

        DB_PASSWORD=yourpassword \\
        VLLM_URL=https://<pod-id>-8000.proxy.runpod.net \\
        python phase3_indication_extraction.py

Environment variables:
    DB_HOST       PostgreSQL host        (default: 178.236.185.230)
    DB_PORT       PostgreSQL port        (default: 5432)
    DB_NAME       Database name          (default: postgres)
    DB_USER       Database user          (default: postgres)
    DB_PASSWORD   Database password      (REQUIRED — no default)
    VLLM_URL      vLLM base URL          (default: http://localhost:8000)
    VLLM_MODEL    Model identifier       (default: Qwen/Qwen2.5-72B-Instruct-AWQ)

Expected runtime and cost:
    ~84,973 drugs / 38 drugs/sec = ~37 minutes at full throughput
    Cost ~$1.10 for the full run (A100 at $1.19/hr)
    Stop your RunPod pod immediately after the script prints DONE.

Architecture:
    - stream_drugs()     : server-side cursor, 500 rows/page, never loads all into RAM
    - feed_queue()       : feeds AsyncIO queue (maxsize=200), closes DB conn when done
    - worker() × 32      : pulls from queue, calls vLLM, writes results under a lock
    - flush every 500    : indication rows + checkpoint written to DB incrementally
    - crash safety       : at most 500 drugs of work lost on crash, checkpoint resumes
"""

# pip install psycopg2-binary aiohttp

import asyncio
import aiohttp
import psycopg2
import psycopg2.extras
import json
import logging
import os
import re
import sys
import time
import datetime
from dataclasses import dataclass
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_HOST     = os.environ.get("DB_HOST",     "localhost")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
DB_NAME     = os.environ.get("DB_NAME",     "postgres")
DB_USER     = os.environ.get("DB_USER",     "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")          # no default — must be set

VLLM_URL   = os.environ.get("VLLM_URL",   "http://localhost:8000")
VLLM_MODEL = os.environ.get("VLLM_MODEL", "Qwen/Qwen2.5-72B-Instruct-AWQ")

CONCURRENCY     = 32
FLUSH_EVERY     = 500   # write to DB every N drugs processed
MAX_RETRIES     = 3
RETRY_DELAY     = 2     # seconds between retries

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("indication_extraction.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System prompt — DO NOT MODIFY
# Must be byte-for-byte identical on every request for prefix caching.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a clinical data extraction assistant. Extract structured data from FDA drug labels. Return ONLY valid JSON. No markdown, no explanation, no backticks.

You will receive FDA label text for a drug. Extract every distinct indication following these rules:
- Each indication is a SEPARATE object in the array
- Use the condition name exactly as written in the label
- icd10: most specific ICD-10-CM code you know, or null
- snomed: SNOMED CT concept ID numeric only, or null
- mesh: MeSH descriptor name, or null
- population: who this applies to — use exactly one of:
  any, adults, pediatric, geriatric, neonates,
  pediatric 2-13y, pediatric 6-17y, treatment-naive,
  treatment-experienced. Use 'any' if not specified.
- line_of_therapy: first-line, second-line, adjunct, salvage, or unspecified
- combination_required: true if label says must be used WITH another drug, false otherwise
- combination_agents: array of other drug names required, or empty array []
- source_excerpt: exact 1-2 sentences from label text that state this indication, under 200 chars

Return a JSON array of objects with exactly these keys:
term, icd10, snomed, mesh, population, line_of_therapy,
combination_required, combination_agents, source_excerpt

Example output format:
[
  {
    "term": "Major Depressive Disorder",
    "icd10": "F32.9",
    "snomed": "35489007",
    "mesh": "Depressive Disorder, Major",
    "population": "adults",
    "line_of_therapy": "unspecified",
    "combination_required": false,
    "combination_agents": [],
    "source_excerpt": "DRUG is indicated for treatment of MDD."
  }
]"""

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DrugRecord:
    formulation_id:       str
    generic_name:         str
    generic_formulation:  str
    openfda_text:         Optional[str]
    openfda_subsections:  Optional[list]
    dailymed_text:        Optional[str]
    dailymed_subsections: Optional[list]


@dataclass
class IndicationRow:
    formulation_id:       str
    term:                 str
    icd10:                Optional[str]
    snomed:               Optional[str]
    mesh:                 Optional[str]
    population:           str
    line_of_therapy:      str
    combination_required: bool
    combination_agents:   List[str]
    source_section:       str
    source_excerpt:       Optional[str]
    source:               str

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    psycopg2.extras.register_default_jsonb(conn, globally=False)
    return conn

# ---------------------------------------------------------------------------
# DB streaming — server-side cursor, 500 rows per page
# ---------------------------------------------------------------------------

FETCH_SQL = """
SELECT
    d.formulation_id,
    d.generic_name,
    d.generic_formulation,
    dml.combined_clean_jsonb
        -> 'openfda' -> 'labeling_content'
        -> 'indications_and_usage' ->> 'text'
        AS openfda_text,
    dml.combined_clean_jsonb
        -> 'openfda' -> 'labeling_content'
        -> 'indications_and_usage' -> 'subsections'
        AS openfda_subsections,
    dml.combined_clean_jsonb
        -> 'dailymed' -> 'labeling_content'
        -> 'indications_and_usage' ->> 'content'
        AS dailymed_text,
    dml.combined_clean_jsonb
        -> 'dailymed' -> 'labeling_content'
        -> 'indications_and_usage' -> 'subsections'
        AS dailymed_subsections
FROM public."DrugMasterLinkage" dml
JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
WHERE (
    dml.combined_clean_jsonb
        -> 'openfda' -> 'labeling_content'
        -> 'indications_and_usage' ->> 'text' IS NOT NULL
    OR
    dml.combined_clean_jsonb
        -> 'dailymed' -> 'labeling_content'
        -> 'indications_and_usage' ->> 'content' IS NOT NULL
)
AND d.generic_formulation IS NOT NULL
AND d.generic_formulation != ''
ORDER BY d.formulation_id
"""

COUNT_SQL = """
SELECT COUNT(*)
FROM public."DrugMasterLinkage" dml
JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
WHERE (
    dml.combined_clean_jsonb
        -> 'openfda' -> 'labeling_content'
        -> 'indications_and_usage' ->> 'text' IS NOT NULL
    OR
    dml.combined_clean_jsonb
        -> 'dailymed' -> 'labeling_content'
        -> 'indications_and_usage' ->> 'content' IS NOT NULL
)
AND d.generic_formulation IS NOT NULL
AND d.generic_formulation != ''
"""


def _parse_jsonb_col(val):
    if val is None:
        return []
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return val if isinstance(val, list) else []


def stream_drugs(conn, already_done: set):
    """
    Server-side cursor — streams DrugRecord objects 500 at a time.
    Skips already_done drugs before yielding so the queue never fills
    with drugs that will just be thrown away.
    """
    with conn.cursor("drug_stream_cursor") as cur:
        cur.itersize = 500
        cur.execute(FETCH_SQL)
        for row in cur:
            (
                formulation_id, generic_name, generic_formulation,
                openfda_text, openfda_subsections,
                dailymed_text, dailymed_subsections,
            ) = row

            if formulation_id in already_done:
                continue  # skip here — never enters the queue

            yield DrugRecord(
                formulation_id       = formulation_id,
                generic_name         = generic_name       or "",
                generic_formulation  = generic_formulation or "",
                openfda_text         = openfda_text,
                openfda_subsections  = _parse_jsonb_col(openfda_subsections),
                dailymed_text        = dailymed_text,
                dailymed_subsections = _parse_jsonb_col(dailymed_subsections),
            )

# ---------------------------------------------------------------------------
# Text merging
# ---------------------------------------------------------------------------

_INCLUDE_KEYWORDS = {"treatment", "indication", "prophylaxis", "use", "approved"}
_EXCLUDE_KEYWORDS = {"limitation", "warning", "safety", "reference", "study"}


def _keep_subsection(title: str) -> bool:
    t = title.lower()
    if any(kw in t for kw in _EXCLUDE_KEYWORDS):
        return False
    return any(kw in t for kw in _INCLUDE_KEYWORDS)


def build_merged_text(drug: DrugRecord) -> Tuple[Optional[str], Optional[str]]:
    """Return (merged_text, source) or (None, None) if no usable text."""

    openfda_parts: List[str] = []
    if drug.openfda_text:
        openfda_parts.append(drug.openfda_text[:3000])
    for sub in (drug.openfda_subsections or []):
        if not isinstance(sub, dict):
            continue
        title, content = sub.get("section_title", ""), sub.get("content", "")
        if title and content and _keep_subsection(title):
            openfda_parts.append(f"[OPENFDA - {title}]\n{content[:500]}")
    openfda_combined = "\n\n".join(openfda_parts) or None

    dailymed_parts: List[str] = []
    if drug.dailymed_text:
        dailymed_parts.append(drug.dailymed_text[:1500])
    for sub in (drug.dailymed_subsections or []):
        if not isinstance(sub, dict):
            continue
        title, content = sub.get("section_title", ""), sub.get("content", "")
        if title and content and _keep_subsection(title):
            dailymed_parts.append(f"[DAILYMED - {title}]\n{content[:500]}")
    dailymed_combined = "\n\n".join(dailymed_parts) or None

    if openfda_combined and dailymed_combined:
        merged = f"[OPENFDA]\n{openfda_combined}\n\n[DAILYMED]\n{dailymed_combined}"
        source = "merged"
    elif openfda_combined:
        merged, source = openfda_combined, "openfda"
    elif dailymed_combined:
        merged, source = dailymed_combined, "dailymed"
    else:
        return None, None

    return merged[:4000], source

# ---------------------------------------------------------------------------
# LLM response parsing
# ---------------------------------------------------------------------------

def parse_llm_response(raw: str, formulation_id: str) -> Optional[list]:
    text = raw.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text).strip()

    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        log.error(f"[{formulation_id}] No JSON array in LLM response")
        return None
    try:
        data = json.loads(match.group())
    except json.JSONDecodeError as exc:
        log.error(f"[{formulation_id}] JSON parse error: {exc}")
        return None
    if not isinstance(data, list):
        log.error(f"[{formulation_id}] LLM response is not a list")
        return None
    return data


def rows_from_parsed(parsed: list, formulation_id: str, source: str) -> List[IndicationRow]:
    rows = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        term = (item.get("term") or "").strip()[:500]
        if not term:
            continue
        agents = item.get("combination_agents", [])
        if not isinstance(agents, list):
            agents = []
        rows.append(IndicationRow(
            formulation_id       = formulation_id,
            term                 = term,
            icd10                = item.get("icd10") or None,
            snomed               = item.get("snomed") or None,
            mesh                 = item.get("mesh") or None,
            population           = (item.get("population") or "any").strip(),
            line_of_therapy      = (item.get("line_of_therapy") or "unspecified").strip(),
            combination_required = bool(item.get("combination_required", False)),
            combination_agents   = agents,
            source_section       = "indications_and_usage",
            source_excerpt       = ((item.get("source_excerpt") or "").strip()[:1000]) or None,
            source               = source,
        ))
    return rows

# ---------------------------------------------------------------------------
# vLLM async call with retry
# ---------------------------------------------------------------------------

async def call_vllm(
    session:     aiohttp.ClientSession,
    semaphore:   asyncio.Semaphore,
    drug:        DrugRecord,
    merged_text: str,
    source:      str,
) -> Tuple[str, List[IndicationRow], int, Optional[str]]:
    """Returns (formulation_id, rows, row_count, error_msg). error_msg is None on success."""

    payload = {
        "model": VLLM_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": (
                f"Extract all indications for {drug.generic_name}.\n\n"
                f"LABEL TEXT:\n{merged_text}"
            )},
        ],
        "temperature": 0.0,
        "max_tokens":  2048,
        "top_p":       1.0,
    }

    last_error: Optional[str] = None

    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.post(f"{VLLM_URL}/v1/chat/completions", json=payload) as resp:
                    if resp.status == 400:
                        body = await resp.text()
                        return drug.formulation_id, [], 0, f"HTTP 400: {body[:200]}"
                    if resp.status in (500, 503):
                        last_error = f"HTTP {resp.status}"
                        if attempt < MAX_RETRIES:
                            await asyncio.sleep(RETRY_DELAY)
                        continue
                    resp.raise_for_status()
                    data   = await resp.json()
                    raw    = data["choices"][0]["message"]["content"]
                    parsed = parse_llm_response(raw, drug.formulation_id)
                    if parsed is None:
                        return drug.formulation_id, [], 0, "json_parse_error"
                    if len(parsed) == 0:
                        log.warning(f"[{drug.formulation_id}] No indications found")
                        return drug.formulation_id, [], 0, "no_indications_found"
                    rows = rows_from_parsed(parsed, drug.formulation_id, source)
                    return drug.formulation_id, rows, len(rows), None

            except asyncio.TimeoutError:
                last_error = "timeout"
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
            except aiohttp.ClientError as exc:
                last_error = str(exc)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)

    return drug.formulation_id, [], 0, f"all_retries_failed: {last_error}"

# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------

def db_insert_indications(conn, rows: List[IndicationRow]) -> None:
    if not rows:
        return
    sql = """
INSERT INTO drugdb.drug_indication
    (formulation_id, term, icd10, snomed, mesh,
     population, line_of_therapy, combination_required,
     combination_agents, source_section, source_excerpt, source)
VALUES %s
ON CONFLICT DO NOTHING
"""
    data = [
        (r.formulation_id, r.term, r.icd10, r.snomed, r.mesh,
         r.population, r.line_of_therapy, r.combination_required,
         r.combination_agents, r.source_section, r.source_excerpt, r.source)
        for r in rows
    ]
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, data)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error(f"db_insert_indications failed: {exc}")
        raise


def db_insert_checkpoint(conn, records: List[Tuple]) -> None:
    """records = list of (formulation_id, status, rows_inserted, error_message)"""
    if not records:
        return
    now  = datetime.datetime.utcnow()
    data = [(fid, status, n, err, now) for fid, status, n, err in records]
    sql  = """
INSERT INTO drugdb.indication_extraction_log
    (formulation_id, status, rows_inserted, error_message, processed_at)
VALUES %s
ON CONFLICT (formulation_id) DO UPDATE
    SET status        = EXCLUDED.status,
        rows_inserted = EXCLUDED.rows_inserted,
        error_message = EXCLUDED.error_message,
        processed_at  = EXCLUDED.processed_at
"""
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, data)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error(f"db_insert_checkpoint failed: {exc}")
        raise

# ---------------------------------------------------------------------------
# Startup checks
# ---------------------------------------------------------------------------

async def startup_checks(session: aiohttp.ClientSession, conn) -> None:
    log.info("Running startup checks...")

    try:
        async with session.get(f"{VLLM_URL}/health", timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                log.error(f"vLLM health check failed: HTTP {r.status}")
                sys.exit(1)
        log.info("vLLM server: OK")
    except Exception as exc:
        log.error(f"vLLM not reachable at {VLLM_URL}: {exc}")
        sys.exit(1)

    try:
        async with session.get(f"{VLLM_URL}/v1/models", timeout=aiohttp.ClientTimeout(total=10)) as r:
            data      = await r.json()
            model_ids = [m["id"] for m in data.get("data", [])]
            if VLLM_MODEL not in model_ids:
                log.error(f"Model '{VLLM_MODEL}' not found. Loaded: {model_ids}")
                sys.exit(1)
        log.info(f"Model verified: {VLLM_MODEL}")
    except Exception as exc:
        log.error(f"Could not verify model list: {exc}")
        sys.exit(1)

    for label, sql in [
        ("drugdb.drug_indication",          "SELECT COUNT(*) FROM drugdb.drug_indication"),
        ("drugdb.indication_extraction_log", "SELECT COUNT(*) FROM drugdb.indication_extraction_log"),
        ('public."DrugMasterLinkage"',       'SELECT COUNT(*) FROM public."DrugMasterLinkage"'),
    ]:
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                count = cur.fetchone()[0]
            if 'DrugMasterLinkage' in label and count == 0:
                log.error("DrugMasterLinkage is empty — run setup SQL first")
                sys.exit(1)
            log.info(f"Table {label}: OK ({count} rows)")
        except Exception as exc:
            log.error(f"Table check failed for {label}: {exc}")
            sys.exit(1)

    log.info("All startup checks passed.")

# ---------------------------------------------------------------------------
# Core extraction — streaming + worker queue + incremental flush
# ---------------------------------------------------------------------------

async def run_extraction(
    conn_read:    psycopg2.extensions.connection,
    conn_write:   psycopg2.extensions.connection,
    already_done: set,
    total_to_proc: int,
    session:      aiohttp.ClientSession,
    semaphore:    asyncio.Semaphore,
    start_time:   float,
) -> Tuple[int, int]:
    """
    Streams all drugs, processes them concurrently, flushes to DB every
    FLUSH_EVERY drugs. Returns (processed_count, total_inserted).

    Design guarantees:
    - already_done filtered in stream_drugs() — never enters queue
    - queue maxsize=200 — RAM stays flat
    - conn_read closed as soon as streaming finishes (~2-3 min)
    - DB flushed every 500 drugs — max 500 drugs lost on crash
    - progress logged every 500 drugs during actual LLM work
    """

    queue: asyncio.Queue = asyncio.Queue(maxsize=200)

    # Shared state — protected by flush_lock
    flush_lock          = asyncio.Lock()
    pending_indications : List[IndicationRow] = []
    pending_checkpoints : List[Tuple]         = []
    processed_count     = 0
    total_inserted      = 0

    # ── Feed task — streams from DB into queue ────────────────────────────
    async def feed_queue() -> None:
        try:
            for drug in stream_drugs(conn_read, already_done):
                await queue.put(drug)
        finally:
            # Always send poison pills even if streaming raises
            for _ in range(CONCURRENCY):
                await queue.put(None)

    # ── Worker — pulls from queue, calls vLLM, flushes every FLUSH_EVERY ─
    async def worker() -> None:
        nonlocal processed_count, total_inserted

        while True:
            drug = await queue.get()
            if drug is None:
                return  # poison pill — this worker is done

            merged, source = build_merged_text(drug)

            if merged is None:
                # No text — checkpoint as done, no LLM call needed
                async with flush_lock:
                    processed_count += 1
                    pending_checkpoints.append(
                        (drug.formulation_id, "done", 0, "no_text_available")
                    )
                    await _maybe_flush()
                continue

            # LLM call — semaphore limits to CONCURRENCY simultaneous
            fid, rows, n_rows, error_msg = await call_vllm(
                session, semaphore, drug, merged, source
            )

            async with flush_lock:
                processed_count += 1
                is_error = (
                    error_msg is not None
                    and error_msg not in ("no_indications_found", "no_text_available")
                )

                if is_error:
                    log.error(f"[{fid}] {error_msg}")
                    pending_checkpoints.append((fid, "error", 0, error_msg))
                else:
                    pending_indications.extend(rows)
                    total_inserted += n_rows
                    pending_checkpoints.append((fid, "done", n_rows, error_msg))

                # Progress every FLUSH_EVERY drugs
                if processed_count % FLUSH_EVERY == 0:
                    elapsed = time.time() - start_time
                    rate    = processed_count / elapsed if elapsed > 0 else 0
                    eta_min = (total_to_proc - processed_count) / rate / 60 if rate > 0 else 0
                    log.info(
                        f"Progress: {processed_count}/{total_to_proc} drugs | "
                        f"{total_inserted} indications | "
                        f"{rate:.1f} drugs/sec | "
                        f"ETA: {eta_min:.1f} min"
                    )

                await _maybe_flush()

    async def _maybe_flush() -> None:
        """Called under flush_lock. Writes to DB when buffer hits FLUSH_EVERY."""
        if len(pending_checkpoints) >= FLUSH_EVERY:
            if pending_indications:
                db_insert_indications(conn_write, pending_indications)
                pending_indications.clear()
            db_insert_checkpoint(conn_write, pending_checkpoints)
            pending_checkpoints.clear()

    # ── Launch feed + workers ─────────────────────────────────────────────
    feed_task    = asyncio.create_task(feed_queue())
    worker_tasks = [asyncio.create_task(worker()) for _ in range(CONCURRENCY)]

    # Close conn_read as soon as streaming finishes — don't hold it 37 min
    await feed_task
    conn_read.close()
    log.info("DB streaming complete — read connection closed")

    # Wait for all workers to finish
    await asyncio.gather(*worker_tasks)

    # Final flush — whatever is left in the buffers
    if pending_indications:
        db_insert_indications(conn_write, pending_indications)
    if pending_checkpoints:
        db_insert_checkpoint(conn_write, pending_checkpoints)

    return processed_count, total_inserted

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    if not DB_PASSWORD:
        log.error("DB_PASSWORD is not set. Exiting.")
        sys.exit(1)

    log.info("=" * 60)
    log.info("Phase 3 — Drug Indication Extraction")
    log.info(f"VLLM_URL:    {VLLM_URL}")
    log.info(f"VLLM_MODEL:  {VLLM_MODEL}")
    log.info(f"CONCURRENCY: {CONCURRENCY}")
    log.info(f"FLUSH_EVERY: {FLUSH_EVERY} drugs")
    log.info("=" * 60)

    connector = aiohttp.TCPConnector(limit=64, limit_per_host=64)
    timeout   = aiohttp.ClientTimeout(connect=10, sock_read=120)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        # Single read connection for startup + streaming
        conn_read = get_connection()

        await startup_checks(session, conn_read)

        log.info("Prefix caching: --enable-prefix-caching on vLLM")
        log.info(f"System prompt length: {len(SYSTEM_PROMPT)} chars (fixed)")

        # Load checkpoint IDs — small query, just UUIDs
        with conn_read.cursor() as cur:
            cur.execute(
                "SELECT formulation_id FROM drugdb.indication_extraction_log "
                "WHERE status = 'done'"
            )
            already_done = {row[0] for row in cur.fetchall()}
        log.info(f"Already done (checkpoint): {len(already_done)}")

        # Count total eligible drugs for ETA
        with conn_read.cursor() as cur:
            cur.execute(COUNT_SQL)
            total_eligible = cur.fetchone()[0]
        total_to_proc = max(0, total_eligible - len(already_done))

        log.info(f"Total eligible drugs: {total_eligible}")
        log.info(f"Remaining to process: {total_to_proc}")
        log.info(f"Estimated time: {total_to_proc / 38 / 60:.1f} hours")
        log.info(f"Estimated cost: ${total_to_proc / 38 / 3600 * 1.19:.2f}")
        log.info("Based on: 38 drugs/sec (awq_marlin, CONCURRENCY=32)")
        log.info("Stop your RunPod pod immediately when done!")

        if total_to_proc == 0:
            log.info("Nothing to process — all drugs already in checkpoint. Exiting.")
            conn_read.close()
            return

        # Separate write connection — stays open for the full run
        conn_write = get_connection()
        semaphore  = asyncio.Semaphore(CONCURRENCY)
        start_time = time.time()

        processed_count, total_inserted = await run_extraction(
            conn_read     = conn_read,    # closed inside run_extraction after streaming
            conn_write    = conn_write,
            already_done  = already_done,
            total_to_proc = total_to_proc,
            session       = session,
            semaphore     = semaphore,
            start_time    = start_time,
        )

        conn_write.close()

        elapsed_sec = time.time() - start_time
        elapsed_min = elapsed_sec / 60

        print("\n" + "=" * 60)
        print("EXTRACTION COMPLETE")
        print(f"Drugs processed this run : {processed_count}")
        print(f"Indications inserted     : {total_inserted}")
        print(f"Avg per drug             : {total_inserted / max(processed_count, 1):.1f}")
        print(f"Total time               : {elapsed_min:.1f} minutes")
        print(f"Estimated cost           : ${elapsed_sec / 3600 * 1.19:.2f}")
        print("=" * 60)
        print()
        print("⚠️  STOP YOUR RUNPOD POD NOW TO AVOID EXTRA CHARGES")
        print("    https://www.runpod.io/console/pods")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())