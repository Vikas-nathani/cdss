import asyncio
import aiohttp
import asyncpg
import psycopg2
import psycopg2.extras
import json
import os
import re
import logging
import sys
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

DB_HOST     = os.environ.get("DB_HOST", "localhost")
DB_PORT     = 5432
DB_NAME     = os.environ.get("DB_NAME", "postgres")
DB_USER     = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY", "")
RUNPOD_URL     = "https://api.runpod.ai/v2/fahewj4m3wv52x/openai/v1/chat/completions"
MODEL_NAME     = "Qwen/Qwen2.5-7B-Instruct"
MAX_WORKERS    = 5

CHECKPOINT_FILE = Path("/home/nathanivikas890_gmail_com/cdss/drug_class_checkpoint.json")
LOG_FILE        = Path("/home/nathanivikas890_gmail_com/cdss/drug_class_extraction.log")
BATCH_SIZE      = 100
STREAM_CHUNK    = 500
MAX_RETRIES     = 1
RETRY_DELAY     = 5
LOG_EVERY       = 500

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ],
)
log = logging.getLogger(__name__)

# ── System Prompt (constant — never modify so vLLM KV cache hits every time) ──

SYSTEM_PROMPT = """You are a pharmaceutical drug classifier.

Given a drug name, its mechanism of action, and its indications,
return a JSON object with exactly these 3 fields:

- pharmacologic_class: list of strings describing the
  pharmacologic class
  e.g. ["Nucleoside Reverse Transcriptase Inhibitor"]
  e.g. ["Atypical Antipsychotic"]
  e.g. ["HMG-CoA Reductase Inhibitor"]

- therapeutic_class: list of strings describing the
  therapeutic or clinical use
  e.g. ["Antiviral", "HIV Treatment"]
  e.g. ["Antipsychotic", "Mood Stabilizer"]
  e.g. ["Lipid Lowering Agent"]

- mechanism_class: list of strings describing the
  mechanism of action
  e.g. ["HIV-1 Reverse Transcriptase Inhibitor"]
  e.g. ["Dopamine D2 and Serotonin 5HT2 Antagonist"]
  e.g. ["Competitive Inhibitor of HMG-CoA Reductase"]

Return ONLY the JSON object.
No explanation. No markdown. No backticks. No extra text.

Example output:
{"pharmacologic_class": ["Nucleoside Reverse Transcriptase Inhibitor"],
 "therapeutic_class": ["Antiviral", "HIV Treatment"],
 "mechanism_class": ["HIV-1 Reverse Transcriptase Inhibitor"]}"""

# ── Checkpoint ────────────────────────────────────────────────────────────────

def load_checkpoint():
    if not CHECKPOINT_FILE.exists():
        return set(), set()
    with open(CHECKPOINT_FILE) as f:
        data = json.load(f)
    completed = set(data.get("completed", []))
    failed = set(data.get("failed", []))
    log.info(f"Checkpoint loaded: {len(completed)} completed, {len(failed)} failed")
    return completed, failed


def save_checkpoint(completed_set, failed_set):
    data = {
        "completed": sorted(completed_set),
        "failed": sorted(failed_set),
        "last_updated": datetime.now().isoformat(),
        "total_completed": len(completed_set),
        "total_failed": len(failed_set),
    }
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)

# ── Parse LLM Response ────────────────────────────────────────────────────────

def parse_llm_response(content):
    try:
        cleaned = re.sub(r"```json\s*|```", "", content).strip()
        parsed = json.loads(cleaned)
        return {
            "pharmacologic_class": parsed.get("pharmacologic_class", []),
            "therapeutic_class":   parsed.get("therapeutic_class", []),
            "mechanism_class":     parsed.get("mechanism_class", []),
        }
    except Exception as e:
        log.warning(f"parse_llm_response failed: {e} | content preview: {content[:120]}")
        return None

# ── RunPod Request ────────────────────────────────────────────────────────────

async def call_runpod(session, drug, semaphore):
    name = drug["generic_name"]
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Classify {name}:\n\n"
                    f"MECHANISM: {drug['mechanism'] or 'not available'}\n"
                    f"INDICATIONS: {drug['indications'] or 'not available'}\n\n"
                    f"Return JSON only:"
                ),
            },
        ],
        "max_tokens": 250,
        "temperature": 0.1,
    }

    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.post(
                    RUNPOD_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=90),
                ) as resp:
                    if resp.status == 200:
                        resp_json = await resp.json()
                        content = resp_json["choices"][0]["message"]["content"]
                        return (name, parse_llm_response(content))
                    else:
                        err = f"HTTP {resp.status}"
            except Exception as e:
                err = str(e)

            log.warning(f"Retry {attempt}/{MAX_RETRIES} for drug: {name}, error: {err}")
            if attempt < MAX_RETRIES:
                if "timeout" in err.lower() or "500" in err or "disconnected" in err.lower():
                    await asyncio.sleep(10)
                else:
                    await asyncio.sleep(RETRY_DELAY)

    return (name, None)

# ── Batch DB Write (synchronous, own connection) ───────────────────────────────

def batch_update_db(results_buffer):
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
        cur = conn.cursor()
        sql = """
            UPDATE drugdb.drug AS d
            SET
                pharmacologic_class = v.pharmacologic_class::text[],
                therapeutic_class   = v.therapeutic_class::text[],
                mechanism_class     = v.mechanism_class::text[],
                drug_class_source   = 'llm'
            FROM (VALUES %s) AS v(
                generic_name,
                pharmacologic_class,
                therapeutic_class,
                mechanism_class
            )
            WHERE d.generic_name = v.generic_name
        """
        psycopg2.extras.execute_values(cur, sql, results_buffer)
        conn.commit()
        log.info(f"DB write: updated {cur.rowcount} formulations for {len(results_buffer)} drugs")
    except Exception as e:
        if conn:
            conn.rollback()
        log.error(f"batch_update_db error: {e}")
    finally:
        if conn:
            conn.close()

# ── Producer ──────────────────────────────────────────────────────────────────

async def producer(drug_queue, completed_set):
    conn = None
    cur = None
    streamed = 0
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
        cur = conn.cursor("drug_class_stream")
        cur.execute("""
            SELECT DISTINCT ON (d.generic_name)
                d.generic_name,
                combined_clean_jsonb
                    -> 'openfda' -> 'clinical'
                    -> 'mechanism_of_action' ->> 'text' AS mechanism,
                combined_clean_jsonb
                    -> 'openfda' -> 'labeling_content'
                    -> 'indications_and_usage' ->> 'text' AS indications
            FROM drugdb.drug d
            JOIN public."DrugMasterLinkage" dml
                ON d.master_linkage_id = dml.master_linkage_id
            WHERE
                combined_clean_jsonb -> 'openfda' IS NOT NULL
                AND d.generic_name IS NOT NULL
                AND TRIM(d.generic_name) != ''
                AND NOT EXISTS (
                    SELECT 1 FROM drugdb.drug d2
                    WHERE d2.generic_name = d.generic_name
                    AND d2.drug_class_source = 'llm'
                )
            ORDER BY d.generic_name
        """)
        while True:
            rows = cur.fetchmany(STREAM_CHUNK)
            if not rows:
                break
            for row in rows:
                generic_name, mechanism, indications = row
                if generic_name in completed_set:
                    continue
                await drug_queue.put({
                    "generic_name": generic_name,
                    "mechanism":    mechanism,
                    "indications":  indications,
                })
                streamed += 1

        log.info(f"Producer finished: {streamed} drugs streamed to queue")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        for _ in range(MAX_WORKERS):
            await drug_queue.put(None)

# ── Consumer ──────────────────────────────────────────────────────────────────

async def consumer(worker_id, drug_queue, result_queue, session, semaphore):
    while True:
        drug = await drug_queue.get()
        if drug is None:
            drug_queue.task_done()
            break
        result = await call_runpod(session, drug, semaphore)
        await result_queue.put(result)
        drug_queue.task_done()
    await result_queue.put(None)
    log.info(f"Consumer {worker_id} finished")

# ── DB Writer ─────────────────────────────────────────────────────────────────

async def db_writer(result_queue, num_workers, completed_set, failed_set):
    loop = asyncio.get_event_loop()
    results_buffer = []
    processed_count = 0
    finished_workers = 0
    start_time = datetime.now()

    while True:
        item = await result_queue.get()

        if item is None:
            finished_workers += 1
            if finished_workers == num_workers:
                break
            continue

        generic_name, result = item

        if result is not None:
            results_buffer.append((
                generic_name,
                result.get("pharmacologic_class", []),
                result.get("therapeutic_class", []),
                result.get("mechanism_class", []),
            ))
            completed_set.add(generic_name)
        else:
            failed_set.add(generic_name)

        processed_count += 1

        if len(results_buffer) >= BATCH_SIZE:
            await loop.run_in_executor(None, batch_update_db, results_buffer)
            results_buffer = []
            save_checkpoint(completed_set, failed_set)

        if processed_count % LOG_EVERY == 0:
            elapsed = max((datetime.now() - start_time).seconds, 1)
            rate = processed_count / elapsed
            log.info(
                f"Progress: {processed_count} processed | "
                f"{len(failed_set)} failed | "
                f"{rate:.1f} drugs/sec"
            )

    if results_buffer:
        await loop.run_in_executor(None, batch_update_db, results_buffer)
    save_checkpoint(completed_set, failed_set)

    elapsed_total = max((datetime.now() - start_time).seconds, 1)
    cost_estimate = processed_count * 0.6 * 0.00044

    log.info("=== COMPLETED ===")
    log.info(f"Total processed:  {processed_count}")
    log.info(f"Total failed:     {len(failed_set)}")
    log.info(f"Total time:       {elapsed_total} seconds")
    log.info(f"Estimated cost:   ${cost_estimate:.4f}")
    log.info("==================")

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info("=== Drug Class Extraction Started ===")
    log.info(f"Timestamp: {datetime.now()}")
    log.info(f"RunPod URL: {RUNPOD_URL}")
    log.info(f"Model: {MODEL_NAME}")
    log.info(f"Max workers: {MAX_WORKERS}")
    log.info(f"Batch size: {BATCH_SIZE}")

    completed_set, failed_set = load_checkpoint()
    log.info(f"Resuming from checkpoint: {len(completed_set)} already done")

    log.info("Starting pipeline - resuming from checkpoint")

    drug_queue   = asyncio.Queue(maxsize=50)
    result_queue = asyncio.Queue(maxsize=200)
    semaphore    = asyncio.Semaphore(MAX_WORKERS)

    headers = {
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
        "Content-Type": "application/json",
    }
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS + 2)

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        await asyncio.gather(
            producer(drug_queue, completed_set),
            consumer(1, drug_queue, result_queue, session, semaphore),
            consumer(2, drug_queue, result_queue, session, semaphore),
            consumer(3, drug_queue, result_queue, session, semaphore),
            consumer(4, drug_queue, result_queue, session, semaphore),
            consumer(5, drug_queue, result_queue, session, semaphore),
            db_writer(result_queue, MAX_WORKERS, completed_set, failed_set),
        )

    log.info("=== Drug Class Extraction Finished ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Script interrupted by user — checkpoint saved")
        sys.exit(0)
    except Exception as e:
        log.error(f"Fatal error: {e}")
        sys.exit(1)
