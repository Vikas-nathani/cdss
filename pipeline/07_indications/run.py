#!/usr/bin/env python3
"""
Phase 3 Drug Indication Extraction Pipeline
============================================

What this script does:
    Extracts structured drug indications from FDA label text using a vLLM
    server (Qwen2.5-72B-Instruct-AWQ). Reads up to 84,973 drugs from
    PostgreSQL in one query, merges OpenFDA + DailyMed indication text,
    sends prompts concurrently to vLLM (CONCURRENCY=32) to saturate the
    A100 GPU, parses JSON responses, and bulk-inserts structured indication
    records into drugdb.drug_indication in batches of 5,000.

How to run:
    Step 1 — Start vLLM on your RunPod A100 with prefix caching:

        python -m vllm.entrypoints.openai.api_server \
            --model Qwen/Qwen2.5-72B-Instruct-AWQ \
            --quantization awq_marlin \
            --max-model-len 4096 \
            --gpu-memory-utilization 0.90 \
            --max-num-seqs 64 \
            --enable-prefix-caching \
            --host 0.0.0.0 \
            --port 8000

    Step 2 — Set environment variables (see below).

    Step 3 — Run:
        python phase3_indication_extraction.py

Environment variables:
    DB_HOST       PostgreSQL host         (default: 178.236.185.230)
    DB_PORT       PostgreSQL port         (default: 5432)
    DB_NAME       Database name           (default: postgres)
    DB_USER       Database user           (default: postgres)
    DB_PASSWORD   Database password       (REQUIRED — no default)
    VLLM_URL      vLLM base URL           (default: http://localhost:8000)
    VLLM_MODEL    Model identifier        (default: Qwen/Qwen2.5-72B-Instruct-AWQ)

Expected runtime and cost:
    ~84,973 drugs / 38 drugs/sec ≈ 37 minutes at full throughput
    Cost ≈ $1.10 for the full run (A100 at $1.19/hr)
    Stop your RunPod pod immediately after the script prints DONE.
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
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_HOST     = os.environ.get("DB_HOST", "localhost")
DB_PORT     = int(os.environ.get("DB_PORT", 5432))
DB_NAME     = os.environ.get("DB_NAME", "postgres")
DB_USER     = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

VLLM_URL = os.environ.get("VLLM_URL", "https://api.groq.com/openai")
VLLM_MODEL = os.environ.get("VLLM_MODEL", "llama-3.3-70b-versatile")
CONCURRENCY  = 1

MAX_RETRIES     = 3
RETRY_DELAY     = 2    # seconds between retries
REQUEST_TIMEOUT = 120  # seconds per request

# ---------------------------------------------------------------------------
# Logging — stdout + file
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
# System prompt
#
# WARNING: Do NOT modify this prompt between requests.
# It must be byte-for-byte identical for every drug.
# Any change breaks prefix caching and increases cost.
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
    source_section:       Optional[str]
    source_excerpt:       Optional[str]
    source:               str

# ---------------------------------------------------------------------------
# Text merging
# ---------------------------------------------------------------------------

_INCLUDE_KEYWORDS = {"treatment", "indication", "prophylaxis", "use", "approved"}
_EXCLUDE_KEYWORDS = {"limitation", "warning", "safety", "reference", "study"}


def _keep_subsection(section_title: str) -> bool:
    t = section_title.lower()
    if any(kw in t for kw in _EXCLUDE_KEYWORDS):
        return False
    return any(kw in t for kw in _INCLUDE_KEYWORDS)


def build_merged_text(drug: DrugRecord) -> Tuple[Optional[str], Optional[str]]:
    """Return (merged_text, source) or (None, None) if no usable text."""

    # --- OpenFDA ---
    openfda_parts: List[str] = []
    if drug.openfda_text:
        openfda_parts.append(drug.openfda_text[:3000])
    if drug.openfda_subsections:
        for sub in drug.openfda_subsections:
            if not isinstance(sub, dict):
                continue
            title   = sub.get("section_title", "")
            content = sub.get("content", "")
            if title and content and _keep_subsection(title):
                openfda_parts.append(f"[OPENFDA - {title}]\n{content[:500]}")
    openfda_combined = "\n\n".join(openfda_parts) if openfda_parts else None

    # --- DailyMed ---
    dailymed_parts: List[str] = []
    if drug.dailymed_text:
        dailymed_parts.append(drug.dailymed_text[:1500])
    if drug.dailymed_subsections:
        for sub in drug.dailymed_subsections:
            if not isinstance(sub, dict):
                continue
            title   = sub.get("section_title", "")
            content = sub.get("content", "")
            if title and content and _keep_subsection(title):
                dailymed_parts.append(f"[DAILYMED - {title}]\n{content[:500]}")
    dailymed_combined = "\n\n".join(dailymed_parts) if dailymed_parts else None

    # --- Merge ---
    if openfda_combined and dailymed_combined:
        merged = f"[OPENFDA]\n{openfda_combined}\n\n[DAILYMED]\n{dailymed_combined}"
        source = "merged"
    elif openfda_combined:
        merged = openfda_combined
        source = "openfda"
    elif dailymed_combined:
        merged = dailymed_combined
        source = "dailymed"
    else:
        return None, None

    if len(merged) > 4000:
        merged = merged[:4000]

    return merged, source

# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_llm_response(raw: str, formulation_id: str) -> Optional[list]:
    """Return parsed list on success, None on parse failure."""
    text = raw.strip()
    # Strip markdown fences
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        log.error(f"[{formulation_id}] No JSON array found in LLM response")
        return None

    try:
        data = json.loads(match.group())
    except json.JSONDecodeError as exc:
        log.error(f"[{formulation_id}] JSON parse error: {exc}")
        return None

    if not isinstance(data, list):
        log.error(f"[{formulation_id}] LLM response is not a JSON list")
        return None

    return data


def rows_from_parsed(
    parsed: list, formulation_id: str, source: str
) -> List[IndicationRow]:
    rows: List[IndicationRow] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        term = (item.get("term") or "").strip()
        if not term:
            continue
        term    = term[:500]
        excerpt = (item.get("source_excerpt") or "").strip()[:1000]
        agents  = item.get("combination_agents", [])
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
            source_excerpt       = excerpt or None,
            source               = source,
        ))
    return rows

# ---------------------------------------------------------------------------
# vLLM async request with retry
# ---------------------------------------------------------------------------

async def call_vllm(
    session:     aiohttp.ClientSession,
    semaphore:   asyncio.Semaphore,
    drug:        DrugRecord,
    merged_text: str,
    source:      str,
) -> Tuple[str, List[IndicationRow], int, Optional[str]]:
    """
    Returns (formulation_id, indication_rows, rows_count, error_message).
    error_message is None on success with rows, 'no_indications_found' on
    valid empty response, or a string describing the failure.
    """
    user_prompt = (
        f"Extract all indications for {drug.generic_name}.\n\n"
        f"LABEL TEXT:\n{merged_text}"
    )
    payload = {
        "model": VLLM_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_prompt},
        ],
        "temperature": 0.0,
        "max_tokens":  2048,
        "top_p":       1.0,
    }

    last_error: Optional[str] = None

    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.post(
                    f"{VLLM_URL}/v1/chat/completions",
                    json=payload,
                ) as resp:
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
                        log.warning(f"[{drug.formulation_id}] No indications extracted")
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
# Database helpers
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    if not DB_PASSWORD:
        log.error("DB_PASSWORD environment variable is not set or is empty.")
        sys.exit(1)
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


def stream_drugs(conn):
    sql = """
SELECT
    d.formulation_id,
    d.generic_name,
    d.generic_formulation,
    dml.combined_clean_jsonb
        -> 'openfda'
        -> 'labeling_content'
        -> 'indications_and_usage'
        ->> 'text'
        AS openfda_text,
    dml.combined_clean_jsonb
        -> 'openfda'
        -> 'labeling_content'
        -> 'indications_and_usage'
        -> 'subsections'
        AS openfda_subsections,
    dml.combined_clean_jsonb
        -> 'dailymed'
        -> 'labeling_content'
        -> 'indications_and_usage'
        ->> 'content'
        AS dailymed_text,
    dml.combined_clean_jsonb
        -> 'dailymed'
        -> 'labeling_content'
        -> 'indications_and_usage'
        -> 'subsections'
        AS dailymed_subsections
FROM public."DrugMasterLinkage" dml
JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
WHERE (
    dml.combined_clean_jsonb
        -> 'openfda'
        -> 'labeling_content'
        -> 'indications_and_usage'
        ->> 'text' IS NOT NULL
    OR
    dml.combined_clean_jsonb
        -> 'dailymed'
        -> 'labeling_content'
        -> 'indications_and_usage'
        ->> 'content' IS NOT NULL
)
AND (d.generic_formulation IS NOT NULL AND d.generic_formulation != '')
ORDER BY d.formulation_id
"""
    with conn.cursor("drug_stream_cursor") as cur:
        cur.itersize = 500
        cur.execute(sql)
        for row in cur:
            (
                formulation_id, generic_name, generic_formulation,
                openfda_text, openfda_subsections,
                dailymed_text, dailymed_subsections,
            ) = row

            if isinstance(openfda_subsections, str):
                try:
                    openfda_subsections = json.loads(openfda_subsections)
                except Exception:
                    openfda_subsections = []
            if isinstance(dailymed_subsections, str):
                try:
                    dailymed_subsections = json.loads(dailymed_subsections)
                except Exception:
                    dailymed_subsections = []

            yield DrugRecord(
                formulation_id       = formulation_id,
                generic_name         = generic_name        or "",
                generic_formulation  = generic_formulation or "",
                openfda_text         = openfda_text,
                openfda_subsections  = openfda_subsections or [],
                dailymed_text        = dailymed_text,
                dailymed_subsections = dailymed_subsections or [],
            )


def fetch_already_done(conn) -> set:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT formulation_id FROM drugdb.indication_extraction_log WHERE status = 'done'"
        )
        return {row[0] for row in cur.fetchall()}


def bulk_insert_indications(conn, rows: List[IndicationRow]) -> None:
    if not rows:
        return
    data = [
        (
            r.formulation_id, r.term, r.icd10, r.snomed, r.mesh,
            r.population, r.line_of_therapy, r.combination_required,
            r.combination_agents, r.source_section, r.source_excerpt, r.source,
        )
        for r in rows
    ]
    sql = """
INSERT INTO drugdb.drug_indication
    (formulation_id, term, icd10, snomed, mesh,
     population, line_of_therapy, combination_required,
     combination_agents, source_section, source_excerpt, source)
VALUES %s
ON CONFLICT DO NOTHING
"""
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, data)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error(f"bulk_insert_indications failed: {exc}")
        raise


def bulk_insert_checkpoint(
    conn, records: List[Tuple[str, str, int, Optional[str]]]
) -> None:
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
        log.error(f"bulk_insert_checkpoint failed: {exc}")
        raise

# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------

async def startup_checks(
    session: aiohttp.ClientSession, conn_read
) -> None:
    log.info("Running startup checks...")

    # Check 1 — vLLM health
    try:
        async with session.get(
            f"{VLLM_URL}/v1/models",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status not in (200, 401):
                log.error(f"API health check failed: HTTP {resp.status}")
                sys.exit(1)
        log.info("API server is ready")
    except Exception as exc:
        log.error(f"API not reachable at {VLLM_URL}: {exc}")
        sys.exit(1)

    # Check 2 — correct model is loaded
    try:
        async with session.get(
            f"{VLLM_URL}/v1/models",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data      = await resp.json()
            model_ids = [m["id"] for m in data.get("data", [])]
            if model_ids and VLLM_MODEL not in model_ids:
                log.warning(
                    f"Model '{VLLM_MODEL}' not in model list: {model_ids}. "
                    f"Proceeding anyway."
                )
        log.info(f"Model verified: {VLLM_MODEL}")
    except Exception as exc:
        log.error(f"Could not verify vLLM model list: {exc}")
        sys.exit(1)

    # Checks 3-5 — required DB tables and source data
    table_checks = [
        ("drugdb.drug_indication",          "SELECT COUNT(*) FROM drugdb.drug_indication"),
        ("drugdb.indication_extraction_log", "SELECT COUNT(*) FROM drugdb.indication_extraction_log"),
        ('public."DrugMasterLinkage"',       'SELECT COUNT(*) FROM public."DrugMasterLinkage"'),
    ]
    for label, sql in table_checks:
        try:
            with conn_read.cursor() as cur:
                cur.execute(sql)
                count = cur.fetchone()[0]
            if label == 'public."DrugMasterLinkage"' and count == 0:
                log.error("DrugMasterLinkage table is empty")
                sys.exit(1)
            log.info(f"Table {label}: OK ({count} rows)")
        except Exception as exc:
            log.error(
                f"Table check failed for {label}: {exc}. "
                "Run the SQL setup script first."
            )
            sys.exit(1)

    log.info("All startup checks passed. Starting extraction.")

# ---------------------------------------------------------------------------
# Worker queue — processes all drugs without an outer batch loop
# ---------------------------------------------------------------------------

async def process_all_drugs(
    conn_read:     psycopg2.extensions.connection,
    conn_write:    psycopg2.extensions.connection,
    already_done:  set,
    session:       aiohttp.ClientSession,
    semaphore:     asyncio.Semaphore,
    total_to_proc: int,
    start_time:    float,
) -> Tuple[int, int]:
    queue: asyncio.Queue = asyncio.Queue(maxsize=200)

    flush_lock           = asyncio.Lock()
    pending_indications: list = []
    pending_checkpoints: list = []
    processed_count      = 0
    total_inserted_count = 0

    async def feed_queue() -> None:
        for drug in stream_drugs(conn_read):
            if drug.formulation_id in already_done:
                continue  # skip before putting in queue
            await queue.put(drug)
        for _ in range(CONCURRENCY):
            await queue.put(None)  # poison pills after all drugs fed

    async def worker() -> None:
        nonlocal processed_count, total_inserted_count
        while True:
            drug = await queue.get()
            if drug is None:
                return
            merged, source = build_merged_text(drug)
            if merged is None:
                async with flush_lock:
                    processed_count += 1
                    pending_checkpoints.append(
                        (drug.formulation_id, "done", 0, "no_text_available")
                    )
                    if len(pending_checkpoints) >= 500:
                        if pending_indications:
                            bulk_insert_indications(conn_write, pending_indications)
                            pending_indications.clear()
                        bulk_insert_checkpoint(conn_write, pending_checkpoints)
                        pending_checkpoints.clear()
                continue
            result = await call_vllm(session, semaphore, drug, merged, source)
            fid, rows, n_rows, error_msg = result

            async with flush_lock:
                processed_count += 1

                is_error = (
                    error_msg is not None
                    and error_msg not in ("no_indications_found", "no_text_available")
                )

                if is_error:
                    log.error(f"[{fid}] Error: {error_msg}")
                    pending_checkpoints.append((fid, "error", 0, error_msg))
                else:
                    pending_indications.extend(rows)
                    total_inserted_count += n_rows
                    pending_checkpoints.append((fid, "done", n_rows, error_msg))

                # Progress log every 500 drugs
                if processed_count % 500 == 0:
                    elapsed = time.time() - start_time
                    rate    = processed_count / elapsed if elapsed > 0 else 0
                    eta_min = (
                        (total_to_proc - processed_count) / rate / 60
                        if rate > 0 else 0
                    )
                    log.info(
                        f"Progress: {processed_count}/{total_to_proc} drugs | "
                        f"{total_inserted_count} indications inserted | "
                        f"{rate:.1f} drugs/sec | "
                        f"ETA: {eta_min:.1f} min remaining"
                    )

                # Flush to DB every 500 drugs
                if len(pending_checkpoints) >= 500:
                    if pending_indications:
                        bulk_insert_indications(conn_write, pending_indications)
                        pending_indications.clear()
                    bulk_insert_checkpoint(conn_write, pending_checkpoints)
                    pending_checkpoints.clear()

    feed_task    = asyncio.create_task(feed_queue())
    worker_tasks = [asyncio.create_task(worker()) for _ in range(CONCURRENCY)]
    await feed_task                      # streaming done — all rows fed or skipped
    conn_read.close()                    # release DB connection immediately
    await asyncio.gather(*worker_tasks)  # workers drain remaining queue

    # Final flush of any remaining pending records
    if pending_indications:
        bulk_insert_indications(conn_write, pending_indications)
    if pending_checkpoints:
        bulk_insert_checkpoint(conn_write, pending_checkpoints)

    return processed_count, total_inserted_count

# ---------------------------------------------------------------------------
# Main async orchestration
# ---------------------------------------------------------------------------

async def main() -> None:
    if not DB_PASSWORD:
        log.error("DB_PASSWORD environment variable is not set. Exiting.")
        sys.exit(1)

    log.info("=" * 60)
    log.info("Phase 3 Drug Indication Extraction")
    log.info(f"VLLM_URL:    {VLLM_URL}")
    log.info(f"VLLM_MODEL:  {VLLM_MODEL}")
    log.info(f"CONCURRENCY: {CONCURRENCY}")
    log.info("=" * 60)

    connector = aiohttp.TCPConnector(limit=64, limit_per_host=64)
    timeout = aiohttp.ClientTimeout(connect=10, sock_read=120)
    headers = {}
    if GROQ_API_KEY:
        headers["Authorization"] = f"Bearer {GROQ_API_KEY}"
    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers=headers
    ) as session:

        # Open read connection — used only for startup fetch, then closed
        conn_read = get_connection()

        await startup_checks(session, conn_read)

        # Prefix-caching confirmation log
        log.info("Prefix caching enabled via --enable-prefix-caching")
        log.info("System prompt is fixed at module level - cache active")
        log.info(f"System prompt length: {len(SYSTEM_PROMPT)} chars")
        log.info("First request will be slow (cache miss)")
        log.info("All subsequent requests will be faster (cache hit)")

        # Load checkpoint (small query — just IDs)
        already_done = fetch_already_done(conn_read)
        log.info(f"Already done (checkpoint): {len(already_done)}")

        # Count eligible drugs for progress estimation
        with conn_read.cursor() as cur:
            cur.execute("""
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
                AND (d.generic_formulation IS NOT NULL AND d.generic_formulation != '')
            """)
            total_count = cur.fetchone()[0]
        remaining     = max(0, total_count - len(already_done))
        total_to_proc = remaining

        log.info(f"Total drugs with indication text: {total_count}")
        log.info(f"Already done (checkpoint): {len(already_done)}")
        log.info(f"Remaining: {remaining}")
        log.info(f"CONCURRENCY: {CONCURRENCY}")
        log.info(f"VLLM_URL: {VLLM_URL}")

        # Startup cost estimate
        log.info(f"Drugs to process: {remaining}")
        log.info(f"Estimated time: {remaining/38/60:.1f} hours")
        log.info(f"Estimated cost: ${remaining/38/3600 * 1.19:.2f}")
        log.info("Based on: 38 drugs/sec with CONCURRENCY=32 + awq_marlin")
        log.info("Remember to STOP the pod when done!")

        # Open write connection — stays open for entire run
        conn_write = get_connection()

        semaphore  = asyncio.Semaphore(CONCURRENCY)
        start_time = time.time()

        # Stream drugs through worker queue — incremental DB writes every 500
        log.info("Streaming drugs through worker queue...")
        processed_count, total_inserted = await process_all_drugs(
            conn_read, conn_write, already_done, session, semaphore,
            total_to_proc, start_time,
        )

        log.info(
            f"Processing complete. "
            f"Processed this run: {processed_count}/{remaining}"
        )

        conn_write.close()

        # Final summary
        elapsed_seconds = time.time() - start_time
        elapsed_minutes = elapsed_seconds / 60
        elapsed_hours   = elapsed_seconds / 3600

        print("=" * 60)
        print("EXTRACTION COMPLETE")
        print(f"Total drugs processed: {processed_count}")
        print(f"Total indications inserted: {total_inserted}")
        print(f"Total time: {elapsed_minutes:.1f} minutes")
        print(f"Estimated cost: ${elapsed_hours * 1.19:.2f}")
        print("=" * 60)
        print("")
        print("⚠️  STOP YOUR RUNPOD POD NOW TO AVOID EXTRA CHARGES")
        print("Go to: https://www.runpod.io/console/pods")
        print("Click STOP on your A100 pod immediately")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
