import requests
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

LOG_FILE = Path("/home/nathanivikas890_gmail_com/cdss/drug_class_test.log")

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

# ── System Prompt (identical to extraction script for KV cache reuse) ─────────

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

# ── Parse ─────────────────────────────────────────────────────────────────────

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
        log.warning(f"Parse failed: {e} | raw: {content[:200]}")
        return None

# ── RunPod (synchronous for test) ─────────────────────────────────────────────

def call_runpod(drug):
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Classify {drug['generic_name']}:\n\n"
                    f"MECHANISM: {drug['mechanism'] or 'not available'}\n"
                    f"INDICATIONS: {drug['indications'] or 'not available'}\n\n"
                    f"Return JSON only:"
                ),
            },
        ],
        "max_tokens": 150,
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
        "Content-Type": "application/json",
    }
    resp = requests.post(RUNPOD_URL, json=payload, headers=headers, timeout=120)
    resp.raise_for_status()
    return resp.json()

# ── Main Test ─────────────────────────────────────────────────────────────────

def main():
    log.info("=== Drug Class Test Started ===")
    log.info(f"Timestamp:  {datetime.now()}")
    log.info(f"RunPod URL: {RUNPOD_URL}")
    log.info(f"Model:      {MODEL_NAME}")
    log.info(f"DB:         {DB_HOST}/{DB_NAME}")

    # Step 1: Fetch 10 drugs from DB
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    cur = conn.cursor()
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
            AND combined_clean_jsonb
                -> 'openfda' -> 'clinical'
                -> 'mechanism_of_action' ->> 'text' IS NOT NULL
        ORDER BY d.generic_name
        LIMIT 10
    """)
    drugs = [
        {"generic_name": row[0], "mechanism": row[1], "indications": row[2]}
        for row in cur.fetchall()
    ]
    log.info(f"Fetched {len(drugs)} drugs from DB")

    runpod_ok = 0
    parse_ok  = 0
    db_ok     = 0
    failed    = 0

    for i, drug in enumerate(drugs):
        name       = drug["generic_name"]
        mechanism  = drug["mechanism"] or ""
        indications = drug["indications"] or ""

        log.info(f"--- Drug {i+1}/10 ---")
        log.info(f"Name:        {name}")
        log.info(f"Mechanism:   {mechanism[:100]}...")
        log.info(f"Indications: {indications[:100]}...")

        # Step 2: Call RunPod
        raw_content = None
        result = None
        try:
            log.info("Sending to RunPod...")
            resp_json = call_runpod(drug)
            raw_content = resp_json["choices"][0]["message"]["content"]
            log.info(f"Raw response: {raw_content}")
            runpod_ok += 1
        except Exception as e:
            log.error(f"RunPod ERROR for {name}: {e}")
            failed += 1
            continue

        # Step 3: Parse
        result = parse_llm_response(raw_content)
        log.info(f"Parsed result: {result}")
        if result is None:
            log.error(f"Parse FAILED for {name} — raw: {raw_content}")
            failed += 1
            continue
        parse_ok += 1

        log.info(f"pharmacologic_class : {result['pharmacologic_class']}")
        log.info(f"therapeutic_class   : {result['therapeutic_class']}")
        log.info(f"mechanism_class     : {result['mechanism_class']}")

        # Step 4: Write to DB
        try:
            cur.execute("""
                UPDATE drugdb.drug
                SET
                    pharmacologic_class = %s,
                    therapeutic_class   = %s,
                    mechanism_class     = %s,
                    drug_class_source   = 'llm'
                WHERE generic_name = %s
            """, (
                result["pharmacologic_class"],
                result["therapeutic_class"],
                result["mechanism_class"],
                name,
            ))
            conn.commit()
            log.info(f"DB updated: {cur.rowcount} rows affected for {name}")
            db_ok += 1
        except Exception as e:
            conn.rollback()
            log.error(f"DB write FAILED for {name}: {e}")
            failed += 1

    cur.close()
    conn.close()

    log.info("=== TEST SUMMARY ===")
    log.info(f"Total drugs tested : 10")
    log.info(f"RunPod successes   : {runpod_ok}")
    log.info(f"Parse successes    : {parse_ok}")
    log.info(f"DB write successes : {db_ok}")
    log.info(f"Failed             : {failed}")
    log.info("====================")


if __name__ == "__main__":
    main()
