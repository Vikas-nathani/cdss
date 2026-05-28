#!/usr/bin/env python3
"""
Phase 3 Step 3.1: Dosing Regimen Extraction Pipeline

Producer/Consumer pattern with 4 async stages running simultaneously.
Nothing is idle: producer feeds fetch_q, 50 API workers drain it into
result_q, a single parser drains result_q into write_q, and the DB writer
drains write_q into PostgreSQL.

Stages:
  1. producer    — queries DB in BATCH_SIZE chunks, enqueues drug rows
  2. api_worker  — calls DeepSeek API (50 concurrent), enqueues parsed JSON
  3. parser      — validates + transforms responses, builds insert tuples
  4. db_writer   — batches inserts into drugdb.dosing_regimen
"""

# =============================================================================
# SECTION 1 — Imports
# =============================================================================
import asyncio
import aiohttp
import asyncpg
import json
import hashlib
import logging
import sys
import os
import random
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
import jsonlines

# =============================================================================
# SECTION 2 — Config constants (all values here — never hardcoded inline)
# =============================================================================
DB_HOST     = os.environ.get("DB_HOST", "localhost")
DB_PORT     = 5432
DB_NAME     = "postgres"
DB_USER     = "postgres"
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

DEEPSEEK_URL     = "https://api.deepseek.com/v1/chat/completions"
MODEL_NAME       = "deepseek-chat"
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")

MAX_WORKERS = 20
BATCH_SIZE  = 200

CHECKPOINT_FILE  = Path("~/cdss/dosing_regimen_checkpoint.json").expanduser()
LOG_FILE         = Path("~/cdss/logs/dosing_regimen_extraction.log").expanduser()
DEAD_LETTER_FILE = Path("~/cdss/logs/dosing_regimen_failed.log").expanduser()
RESULT_LOG_FILE  = Path("~/cdss/logs/dosing_regimen_results.log").expanduser()
RESPONSE_CACHE_PATH = str(Path("~/cdss/logs/dosing_regimen_responses.jsonl").expanduser())

LOG_EVERY     = 500    # log progress every N drugs
MAX_RETRIES   = 2
DRY_RUN_LIMIT = 500    # set to None for full run

COST_PER_DRUG   = 0.000211  # USD per drug (DeepSeek deepseek-chat estimated cost)
WRITE_FLUSH_SIZE = 1000      # flush DB buffer when this many rows are buffered
TOTAL_DRUGS      = 47481     # known total for ETA estimation

# =============================================================================
# SECTION 3 — Logging setup (console + file simultaneously)
# =============================================================================

def setup_logging() -> logging.Logger:
    """Configure dual logging to stdout and LOG_FILE. Creates log dir if needed."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(LOG_FILE), mode="a", encoding="utf-8"),
        ],
    )
    return logging.getLogger(__name__)


logger = setup_logging()

# Separate dead-letter logger — failed master_linkage_ids go here for later reprocessing
DEAD_LETTER_FILE.parent.mkdir(parents=True, exist_ok=True)
_dl_handler = logging.FileHandler(str(DEAD_LETTER_FILE), mode="a", encoding="utf-8")
_dl_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
dead_letter = logging.getLogger("dead_letter")
dead_letter.addHandler(_dl_handler)
dead_letter.setLevel(logging.ERROR)
dead_letter.propagate = False  # do not echo dead-letter entries to the main log

# Result logger — logs every extracted row even if DB insert fails
RESULT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
_result_handler = logging.FileHandler(str(RESULT_LOG_FILE), mode="a", encoding="utf-8")
_result_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
result_logger = logging.getLogger("result_logger")
result_logger.addHandler(_result_handler)
result_logger.setLevel(logging.INFO)
result_logger.propagate = False

# =============================================================================
# SECTION 4 — System prompt  (CACHE THIS — identical on every API call)
# DeepSeek automatically caches the system prompt prefix across requests,
# reducing cost and latency. Never modify this string mid-run.
# =============================================================================

SYSTEM_PROMPT = """You are a clinical pharmacology data extractor. Extract ALL dosing regimens from the label text below.

OUTPUT RULES:
1. Return ONLY a valid JSON array. No markdown, no commentary, no explanation.
2. Each array element is one dosing regimen row.
3. If a field value is unknown or not mentioned, use null.
4. Never invent or infer values not stated in the text.
5. For dose_value: always extract a number. Never return null if
   dose_amount contains any number.
   - Range "150-750 mg/day" → dose_value = 150 (always use lower bound)
   - Range "60 mg/m2 to 100 mg/m2" → dose_value = 60 (lower bound)
   - Exact "100 mg" → dose_value = 100
   - For dose_unit: always populate if dose_amount contains a unit.
   - "Twice daily dosing totaling 100 mg" → dose_value = 50 (always extract per-dose value = total ÷ frequency)
   - "totaling X mg" with BID → dose_value = X/2
   - "totaling X mg" with TID → dose_value = X/3
   - "totaling X mg" with QID → dose_value = X/4
8. source_section: always return null. Never populate this field.
9. source_excerpt: always return null. Never populate this field.

ROW GRANULARITY — create a SEPARATE row for each unique combination of:
  indication × age group × renal impairment tier × hepatic impairment tier × route

CONTROLLED VOCABULARY:
  age_group:        neonate | infant | pediatric | adolescent | adult | geriatric | any
  - "elderly", "older adults", "older patients", "seniors" → use "geriatric"
  sex:              any | male | female
  pregnancy_status: any | pregnant | not_pregnant | lactating
  - "postpartum_nonlactating", "postpartum non-lactating", "postpartum" → use "not_pregnant"
  renal_function:   any | normal | mild_impairment | moderate_impairment | severe_impairment | esrd
  - "dialysis", "on dialysis", "hemodialysis", "peritoneal dialysis" → use "esrd"
  - "impaired elimination", "renal impairment" (unspecified) → use "severe_impairment"
  hepatic_function: any | normal | mild_impairment | moderate_impairment | severe_impairment
  dose_basis:       fixed | per_kg | per_m2 | titrated
  frequency:        QD | BID | TID | QID | q6h | q8h | q12h | q4-6h | q48h | q3w | q4w | weekly | biweekly | monthly | once | as_needed | continuous
  - "every other day", "QOD", "every_other_day" → use "q48h"
  - "every 3 weeks", "every 21 days", "q21d" → use "q3w"
  - "every 4 weeks", "q4w", "every 28 days" → use "q4w"
  - "every 12 hours" → use "q12h"
  - "every 4-6 hours as needed", "q4-6h prn" → use "q4-6h"
  - "continuous", "continuous infusion", "continuous IV" → use "continuous"
  - Never output free text — always map to the closest controlled vocabulary term above

FREQUENCY MAPPING RULES (always map free text to controlled vocab):
  - "every other day", "QOD", "every_other_day", "every 48 hours" → q48h
  - "every 3 weeks", "every 21 days", "q21d", "q3 weeks" → q3w
  - "every 4 weeks", "every 28 days", "q28d" → q4w
  - "every 6 hours", "q6 hours", "every 6h" → q6h
  - "every 8 hours", "q8 hours", "every 8h" → q8h
  - "every 12 hours", "q12 hours", "every 12h" → q12h
  - "every 4 to 6 hours as needed", "q4-6h prn", "every 4-6 hours" → q4-6h
  - "continuous infusion", "continuous IV", "continuous drip" → continuous
  - "four times daily", "four times a day" → QID
  - "three times daily", "three times a day" → TID
  - "twice daily", "two times daily", "two times a day" → BID
  - "once daily", "once a day", "one time daily" → QD
  - "once weekly", "one time per week" → weekly
  - "twice weekly", "two times per week" → biweekly
  - "once monthly", "one time per month" → monthly
  - "as needed", "prn", "when needed", "when necessary" → as_needed
  - "single dose", "one time dose", "one-time" → once

AGE GROUP MAPPING RULES:
  - "elderly", "older adults", "older patients", "seniors", "aged" → geriatric
  - "neonates", "newborns", "newborn infants" → neonate
  - "infants", "babies" → infant
  - "children", "pediatric patients", "kids" → pediatric
  - "adolescents", "teenagers", "teens" → adolescent
  - "adults", "adult patients" → adult
  - "all patients", "patients" → any

RENAL FUNCTION MAPPING RULES:
  - "normal renal function", "normal kidney function" → normal
  - "mild renal impairment", "mild kidney impairment", "CrCl 60-89" → mild_impairment
  - "moderate renal impairment", "moderate kidney impairment", "CrCl 30-59" → moderate_impairment
  - "severe renal impairment", "severe kidney impairment", "CrCl <30", "impaired elimination" → severe_impairment
  - "end-stage renal disease", "ESRD", "dialysis", "hemodialysis", "peritoneal dialysis", "on dialysis" → esrd
  - "no renal restriction", "any renal status" → any

HEPATIC FUNCTION MAPPING RULES:
  - "normal hepatic function", "normal liver function" → normal
  - "mild hepatic impairment", "mild liver impairment", "Child-Pugh A" → mild_impairment
  - "moderate hepatic impairment", "moderate liver impairment", "Child-Pugh B" → moderate_impairment
  - "severe hepatic impairment", "severe liver impairment", "Child-Pugh C" → severe_impairment
  - "any hepatic status", "no hepatic restriction" → any

PREGNANCY STATUS MAPPING RULES:
  - "pregnant women", "pregnancy", "during pregnancy" → pregnant
  - "non-pregnant", "not pregnant", "postpartum_nonlactating", "postpartum non-lactating" → not_pregnant
  - "breastfeeding", "nursing mothers", "lactating women", "lactation" → lactating
  - "all patients regardless of pregnancy" → any

DOSE BASIS MAPPING RULES:
  - "flat dose", "fixed dose", "absolute dose" → fixed
  - "per kilogram", "per kg body weight", "weight-based" → per_kg
  - "per square meter", "per m2", "per body surface area", "BSA-based" → per_m2
  - "titrated to response", "dose titration", "titrate based on" → titrated

CONTRAINDICATION: If a population is explicitly contraindicated set dose_amount="CONTRAINDICATED"

JSON SCHEMA each row must have ALL of these keys:
{
  "indication": string | null,
  "population": {
    "age_group": string,
    "age_min_years": number | null,
    "age_max_years": number | null,
    "weight_min_kg": number | null,
    "weight_max_kg": number | null,
    "sex": string,
    "pregnancy_status": string,
    "renal_function": string,
    "hepatic_function": string
  },
  "route": string | null,
  "dose_amount": string | null,
  "dose_value": number | null,
  "dose_unit": string | null,
  "dose_basis": string | null,
  "frequency": string | null,
  "duration": string | null,
  "max_daily_dose": string | null,
  "administration_notes": string | null,
  "adjustment_required_for": string[],
  "source_section": string | null,
  "source_excerpt": string | null
}"""

# =============================================================================
# SECTION 5 — Checkpoint functions
# =============================================================================

def load_checkpoint() -> dict:
    """Load checkpoint from file. Returns dict with processed set and offset.
    Returns default empty state if no checkpoint exists or file is corrupt.
    """
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                data = json.load(f)
            data["processed_ids"] = set(data.get("processed_ids", []))
            logger.info(
                f"Checkpoint loaded: offset={data.get('offset', 0)}, "
                f"processed={len(data['processed_ids'])}"
            )
            return data
        except Exception as e:
            logger.warning(f"Checkpoint load failed ({e}). Starting fresh.")
    return {
        "processed_ids": set(),
        "offset": 0,
        "stats": {"processed": 0, "inserted": 0, "failed": 0},
    }


def save_checkpoint(processed_ids: set, offset: int, stats: dict):
    """Save checkpoint atomically (write to .tmp then rename so a crash mid-write
    never corrupts the checkpoint file).
    """
    tmp = CHECKPOINT_FILE.with_suffix(".json.tmp")
    payload = {
        "processed_ids": list(processed_ids),
        "offset": offset,
        "stats": {k: v for k, v in stats.items() if k != "start_time"},
        "saved_at": datetime.utcnow().isoformat(),
    }
    try:
        with open(str(tmp), "w") as f:
            json.dump(payload, f)
        tmp.rename(CHECKPOINT_FILE)
    except Exception as e:
        logger.error(f"Checkpoint save failed: {e}")

# =============================================================================
# SECTION 6 — Helper functions
# =============================================================================

def make_regimen_id(formulation_id: str, row: dict) -> str:
    """Generate a deterministic unique ID for a dosing regimen row.

    Hashes: formulation_id + indication + age_group + renal_function
            + hepatic_function + route + sex + pregnancy_status.
    Uses MD5 and returns the first 16 hex characters.
    Collision-resistant enough for ~millions of regimen rows.
    """
    pop = row.get("population") or {}
    key = "|".join([
        str(formulation_id or ""),   # uuid.UUID → str for hashing
        str(row.get("indication") or ""),
        str(pop.get("age_group") or "any"),
        str(pop.get("renal_function") or "any"),
        str(pop.get("hepatic_function") or "any"),
        str(row.get("route") or ""),
        str(pop.get("sex") or "any"),
        str(pop.get("pregnancy_status") or "any"),
    ])
    return hashlib.md5(key.encode()).hexdigest()[:16]


def build_payload(drug_name: str, openfda_text: str, dailymed_text: str) -> dict:
    """Build the DeepSeek API request payload.

    System prompt is identical on every call so DeepSeek's prefix cache
    is hit on requests 2+, reducing cost and latency significantly.
    """
    return {
        "model": MODEL_NAME,
        "max_tokens": 8192,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Drug: {drug_name}\n\n"
                    f"[OpenFDA]\n{openfda_text or ''}\n\n"
                    f"[DailyMed]\n{dailymed_text or ''}"
                ),
            },
        ],
    }


# Required keys for row-level and population-level validation
_REQUIRED_ROW_KEYS = {
    "indication", "population", "route", "dose_amount", "dose_value",
    "dose_unit", "dose_basis", "frequency", "duration", "max_daily_dose",
    "administration_notes", "adjustment_required_for",
    "source_section", "source_excerpt",
}
_REQUIRED_POP_KEYS = {
    "age_group", "age_min_years", "age_max_years",
    "weight_min_kg", "weight_max_kg",
    "sex", "pregnancy_status", "renal_function", "hepatic_function",
}
_POP_STRING_KEYS = {"age_group", "sex", "pregnancy_status", "renal_function", "hepatic_function"}


def safe_numeric(v):
    """Coerce value to float for NUMERIC DB columns.
    Uses Decimal for string parsing to avoid float precision issues
    e.g. float('2.2') = 2.2000000017... but Decimal('2.2') is exact.
    """
    if v is None:
        return None
    try:
        return float(Decimal(str(v)))
    except (InvalidOperation, TypeError, ValueError):
        return None


def validate_and_parse(response_data: dict) -> tuple:
    """Parse the LLM response into a list of validated dosing rows.

    Handles two response shapes:
      - JSON array  (the model followed instructions exactly)
      - JSON object (json_object mode wraps the array; finds the first list value)

    Fills missing keys with safe defaults. Returns empty list on any failure —
    never raises, always logs the error so the pipeline can continue.
    """
    try:
        content = response_data["choices"][0]["message"]["content"]
        logger.debug("RAW CONTENT (first 500): %s", repr(content[:500]))
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[-1]
            content = content.rsplit("```", 1)[0].strip()
        parsed = json.loads(content)

        if isinstance(parsed, list):
            rows = parsed
        elif isinstance(parsed, dict):
            # json_object mode: find first key whose value is a list
            rows = next((v for v in parsed.values() if isinstance(v, list)), [])
        else:
            logger.debug("validate_and_parse: unexpected top-level type %s", type(parsed))
            return []

        validated = []
        for row in rows:
            if not isinstance(row, dict):
                continue

            # Fill missing top-level keys
            for k in _REQUIRED_ROW_KEYS:
                if k not in row:
                    row[k] = [] if k == "adjustment_required_for" else None

            # Ensure population sub-object exists and is complete
            if not isinstance(row.get("population"), dict):
                row["population"] = {}
            pop = row["population"]
            for k in _REQUIRED_POP_KEYS:
                if k not in pop:
                    pop[k] = "any" if k in _POP_STRING_KEYS else None

            # Coerce adjustment_required_for to a plain list
            if not isinstance(row.get("adjustment_required_for"), list):
                row["adjustment_required_for"] = []

            # Coerce all numeric fields — LLM may return them as strings
            row["dose_value"]         = safe_numeric(row.get("dose_value"))
            pop["age_min_years"]      = safe_numeric(pop.get("age_min_years"))
            pop["age_max_years"]      = safe_numeric(pop.get("age_max_years"))
            pop["weight_min_kg"]      = safe_numeric(pop.get("weight_min_kg"))
            pop["weight_max_kg"]      = safe_numeric(pop.get("weight_max_kg"))

            validated.append(row)

        return validated, False

    except json.JSONDecodeError as e:
        logger.warning(f"validate_and_parse error: {e}")

        # PARTIAL RECOVERY — scan objects one by one using raw_decode
        # This correctly handles nested objects unlike rfind(},)
        try:
            decoder = json.JSONDecoder()
            recovered_rows = []
            scan_pos = 0

            # Skip opening bracket
            content_stripped = content.strip()
            if content_stripped.startswith('['):
                scan_pos = 1

            while scan_pos < len(content_stripped):
                # Skip whitespace and commas between objects
                while scan_pos < len(content_stripped) and content_stripped[scan_pos] in ' \n\r\t,':
                    scan_pos += 1

                if scan_pos >= len(content_stripped):
                    break

                if content_stripped[scan_pos] == ']':
                    break

                try:
                    obj, end_pos = decoder.raw_decode(content_stripped, scan_pos)
                    recovered_rows.append(obj)
                    scan_pos = end_pos
                except json.JSONDecodeError:
                    # Hit truncation point — stop here
                    break

            if recovered_rows:
                logger.info(
                    f"validate_and_parse: partial recovery succeeded — "
                    f"recovered {len(recovered_rows)} rows from truncated response"
                )
                return recovered_rows, True

        except Exception as recovery_error:
            logger.debug(f"validate_and_parse: partial recovery failed: {recovery_error}")

        return None, False

    except Exception as e:
        logger.warning(f"validate_and_parse error: {e}")
        return [], False


def exponential_backoff(attempt: int) -> float:
    """Return seconds to wait before the next retry: min(2^attempt + jitter, 60)."""
    return min(2 ** attempt + random.uniform(0, 5), 60.0)

# =============================================================================
# SECTION 7 — SQL queries (all SQL as named constants — never inline)
# =============================================================================

FETCH_SQL = """
    WITH already_done AS (
        SELECT DISTINCT d2.master_linkage_id
        FROM drugdb.drug d2
        INNER JOIN drugdb.dosing_regimen dr2
            ON dr2.formulation_id = d2.formulation_id
        UNION
        SELECT master_linkage_id
        FROM drugdb.failed_drugs
        WHERE failure_reason IN ('ZERO_ROWS', 'DEFERRED', 'PARTIAL')
    )
    SELECT DISTINCT ON (dml.master_linkage_id)
        dml.master_linkage_id,
        d.generic_name,
        dml.combined_clean_jsonb -> 'openfda'
            -> 'labeling_content'
            -> 'dosage_and_administration'
            ->> 'text'    AS openfda_text,
        dml.combined_clean_jsonb -> 'dailymed'
            -> 'labeling_content'
            -> 'dosage_and_administration'
            ->> 'content' AS dailymed_text
    FROM public."DrugMasterLinkage" dml
    JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
    WHERE (
        (dml.combined_clean_jsonb -> 'openfda'
             -> 'labeling_content'
             -> 'dosage_and_administration'
             ->> 'text' IS NOT NULL
         AND TRIM(dml.combined_clean_jsonb -> 'openfda'
             -> 'labeling_content'
             -> 'dosage_and_administration'
             ->> 'text') != '')
        OR
        (dml.combined_clean_jsonb -> 'dailymed'
             -> 'labeling_content'
             -> 'dosage_and_administration'
             ->> 'content' IS NOT NULL
         AND TRIM(dml.combined_clean_jsonb -> 'dailymed'
             -> 'labeling_content'
             -> 'dosage_and_administration'
             ->> 'content') != '')
    )
    AND dml.master_linkage_id NOT IN (
        SELECT master_linkage_id FROM already_done
        WHERE master_linkage_id IS NOT NULL
    )
    ORDER BY dml.master_linkage_id
    LIMIT $1
"""

FORMULATIONS_SQL = """
    SELECT formulation_id
    FROM drugdb.drug
    WHERE master_linkage_id = $1
"""

INSERT_SQL = """
    INSERT INTO drugdb.dosing_regimen (
        regimen_id, formulation_id, indication,
        age_group, age_min_years, age_max_years,
        weight_min_kg, weight_max_kg, sex, pregnancy_status,
        renal_function, hepatic_function, route,
        dose_amount, dose_value, dose_unit, dose_basis,
        frequency, duration, max_daily_dose,
        administration_notes, adjustment_required_for,
        source_section, source_excerpt
    ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24
    )
    ON CONFLICT (regimen_id) DO NOTHING
"""

# =============================================================================
# SECTION 8 — Four pipeline stages
# =============================================================================

async def producer(
    pool: asyncpg.Pool,
    fetch_q: asyncio.Queue,
    limit: int | None = None,
):
    """Stage 1: Fetches drug records from the DB in BATCH_SIZE chunks and
    enqueues them for the API workers. Resumes from checkpoint offset.

    Puts exactly ONE None sentinel into fetch_q when done. Each api_worker
    that receives None passes it forward so all 50 workers stop cleanly.
    """
    checkpoint = load_checkpoint()
    total_queued = 0
    queued_this_run = set()
    in_flight_since: float | None = None
    IN_FLIGHT_TIMEOUT = 600  # 10 minutes

    logger.info("Producer started")

    while True:
        batch_limit = BATCH_SIZE
        if limit is not None:
            remaining = limit - total_queued
            if remaining <= 0:
                break
            batch_limit = min(BATCH_SIZE, remaining)

        try:
            async with pool.acquire() as conn:
                raw_rows = await conn.fetch(FETCH_SQL, batch_limit)
        except Exception as e:
            logger.error(f"Producer: DB fetch error: {e}")
            break

        if not raw_rows:
            logger.info("Producer: no more rows — all drugs queued")
            break

        rows = [r for r in raw_rows if r['master_linkage_id'] not in queued_this_run]
        for r in rows:
            queued_this_run.add(r['master_linkage_id'])

        if not rows:
            # All fetched rows are in-flight (queued but not yet committed to DB).
            # Sleep and retry — the already_done CTE will exclude them once committed.
            now = asyncio.get_event_loop().time()
            if in_flight_since is None:
                in_flight_since = now
            elif now - in_flight_since > IN_FLIGHT_TIMEOUT:
                logger.warning(
                    f"Producer: {len(raw_rows)} drugs stuck in-flight for >{IN_FLIGHT_TIMEOUT}s "
                    f"without committing — likely lost. Breaking to avoid infinite wait."
                )
                break
            logger.info(
                f"Producer: all {len(raw_rows)} fetched rows in-flight, waiting 15s "
                f"(elapsed: {int(now - in_flight_since)}s / {IN_FLIGHT_TIMEOUT}s timeout)..."
            )
            await asyncio.sleep(15)
            continue

        in_flight_since = None  # reset timer whenever new rows are found
        logger.info(f"Producer: fetched batch of {len(rows)} new (raw: {len(raw_rows)})")

        for row in rows:
            await fetch_q.put(dict(row))
            total_queued += 1

    for _ in range(MAX_WORKERS):
        await fetch_q.put(None)
    logger.info(f"Producer done — {total_queued} total records queued")


async def api_worker(
    session: aiohttp.ClientSession,
    pool: asyncpg.Pool,
    fetch_q: asyncio.Queue,
    result_q: asyncio.Queue,
    worker_id: int,
):
    """Stage 2: Consumes drug rows from fetch_q, calls the DeepSeek API,
    and enqueues (master_linkage_id, generic_name, response_data) to result_q.

    On 429 rate-limit: retries with exponential backoff up to MAX_RETRIES times.
    On any other non-200: logs to dead-letter and continues.
    On None sentinel: passes it forward so the next worker also stops, then exits.
    Never crashes the pipeline regardless of error type.
    """
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }

    while True:
        row = await fetch_q.get()
        try:
            if row is None:
                await result_q.put(None)
                return

            master_linkage_id = row["master_linkage_id"]
            generic_name      = row.get("generic_name", "")
            openfda_text      = row.get("openfda_text") or ""
            dailymed_text     = row.get("dailymed_text") or ""

            payload          = build_payload(generic_name, openfda_text, dailymed_text)
            response_data    = None
            last_http_status = None

            for attempt in range(MAX_RETRIES):
                try:
                    async with session.post(DEEPSEEK_URL, json=payload, headers=headers) as resp:
                        if resp.status in (429, 500, 502, 503):
                            wait_secs = exponential_backoff(attempt)
                            logger.warning(
                                f"Worker {worker_id}: {resp.status} rate limit or server error for "
                                f"{master_linkage_id} ({generic_name}), "
                                f"retry {attempt + 1}/{MAX_RETRIES}, "
                                f"backoff {wait_secs:.1f}s"
                            )
                            await asyncio.sleep(wait_secs)
                            continue

                        if resp.status != 200:
                            last_http_status = resp.status
                            body = await resp.text()
                            logger.error(
                                f"Worker {worker_id}: HTTP {resp.status} for "
                                f"{master_linkage_id}: {body[:300]}"
                            )
                            break

                        response_data = await resp.json()
                        # Save raw response to JSONL cache
                        try:
                            with jsonlines.open(RESPONSE_CACHE_PATH, mode='a') as writer:
                                writer.write({
                                    "master_linkage_id": str(master_linkage_id),
                                    "generic_name": generic_name,
                                    "timestamp": datetime.utcnow().isoformat(),
                                    "raw_response": response_data["choices"][0]["message"]["content"]
                                })
                        except Exception as je:
                            logger.debug(f"JSONL cache write error: {je}")
                        break  # success

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    wait_secs = exponential_backoff(attempt)
                    logger.warning(
                        f"Worker {worker_id}: network error for {master_linkage_id} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES}): {e}, "
                        f"retrying in {wait_secs:.1f}s"
                    )
                    await asyncio.sleep(wait_secs)

            if response_data is None:
                logger.error(
                    f"Worker {worker_id}: all {MAX_RETRIES} retries exhausted for "
                    f"{master_linkage_id} ({generic_name})"
                )
                dead_letter.error(
                    f"FAILED | {master_linkage_id} | {generic_name} | exhausted_retries"
                )
                # 402 = balance ran out — drug is fine, will succeed next run, do not blacklist
                if last_http_status != 402:
                    try:
                        async with pool.acquire() as conn:
                            await conn.execute("""
                                INSERT INTO drugdb.failed_drugs
                                    (master_linkage_id, generic_name, failure_reason)
                                VALUES ($1, $2, 'EXHAUSTED_RETRIES')
                                ON CONFLICT (master_linkage_id) DO UPDATE
                                SET attempt_count = failed_drugs.attempt_count + 1,
                                    failed_at = NOW()
                            """, master_linkage_id, generic_name)
                    except Exception as fe:
                        logger.warning(f"Worker: failed_drugs insert error: {fe}")
                continue

            await result_q.put((master_linkage_id, generic_name, response_data))

        except Exception as e:
            logger.error(f"Worker {worker_id}: unexpected error: {e}", exc_info=True)
        finally:
            fetch_q.task_done()


async def parser(
    pool: asyncpg.Pool,
    result_q: asyncio.Queue,
    write_q: asyncio.Queue,
):
    """Stage 3: Consumes API responses from result_q, validates and transforms
    them, fetches all formulation_ids for the drug from the DB, then builds
    a list of 24-tuple insert records and enqueues it to write_q.

    Tracks completion of all MAX_WORKERS api_workers via sentinel counting.
    When all workers have sent their None, puts a single None to write_q.
    """
    workers_done = 0
    while True:
        item = await result_q.get()
        try:
            if item is None:
                workers_done += 1
                if workers_done == MAX_WORKERS:
                    await write_q.put(None)
                    return
                continue

            master_linkage_id, generic_name, response_data = item

            rows, is_partial = validate_and_parse(response_data)

            if not rows:
                logger.warning(
                    f"Parser: zero rows extracted for {generic_name} ({master_linkage_id})"
                )
                dead_letter.error(
                    f"ZERO_ROWS | {master_linkage_id} | {generic_name}"
                )
                try:
                    async with pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO drugdb.failed_drugs
                                (master_linkage_id, generic_name, failure_reason)
                            VALUES ($1, $2, 'ZERO_ROWS')
                            ON CONFLICT (master_linkage_id) DO UPDATE
                            SET attempt_count = failed_drugs.attempt_count + 1,
                                failed_at = NOW()
                        """, master_linkage_id, generic_name)
                except Exception as fe:
                    logger.warning(f"Parser: failed_drugs insert error: {fe}")
                # Log ZERO_ROWS status to JSONL
                try:
                    with jsonlines.open(RESPONSE_CACHE_PATH, mode='a') as writer:
                        writer.write({
                            "master_linkage_id": str(master_linkage_id),
                            "generic_name": generic_name,
                            "timestamp": datetime.utcnow().isoformat(),
                            "status": "ZERO_ROWS",
                            "rows_extracted": 0
                        })
                except Exception as je:
                    logger.debug(f"JSONL cache write error: {je}")
                continue

            if len(rows) > 100:
                logger.warning(
                    f"Parser: unusually high row count ({len(rows)}) "
                    f"for {generic_name} ({master_linkage_id}) — inserting anyway"
                )

            # Fetch all formulation_ids tied to this master_linkage_id
            try:
                async with pool.acquire() as conn:
                    formulations = await conn.fetch(FORMULATIONS_SQL, master_linkage_id)
            except Exception as e:
                logger.error(
                    f"Parser: formulation fetch failed for {master_linkage_id}: {e}"
                )
                dead_letter.error(
                    f"FORMULATION_FETCH_FAILED | {master_linkage_id} | {generic_name} | {e}"
                )
                continue

            if not formulations:
                logger.warning(
                    f"Parser: no formulations for {master_linkage_id} ({generic_name}) — skipping"
                )
                continue

            insert_records = []
            for row in rows:
                pop = row.get("population") or {}
                for form in formulations:
                    fid = form["formulation_id"]
                    insert_records.append((
                        make_regimen_id(fid, row),              # $1  regimen_id
                        fid,                                    # $2  formulation_id
                        ", ".join(row.get("indication")) if isinstance(row.get("indication"), list) else row.get("indication"),  # $3  indication
                        pop.get("age_group", "any"),            # $4  age_group
                        pop.get("age_min_years"),               # $5  age_min_years
                        pop.get("age_max_years"),               # $6  age_max_years
                        pop.get("weight_min_kg"),               # $7  weight_min_kg
                        pop.get("weight_max_kg"),               # $8  weight_max_kg
                        pop.get("sex", "any"),                  # $9  sex
                        pop.get("pregnancy_status", "any"),     # $10 pregnancy_status
                        pop.get("renal_function", "any"),       # $11 renal_function
                        pop.get("hepatic_function", "any"),     # $12 hepatic_function
                        row.get("route"),                       # $13 route
                        row.get("dose_amount"),                 # $14 dose_amount
                        row.get("dose_value"),                  # $15 dose_value
                        row.get("dose_unit"),                   # $16 dose_unit
                        row.get("dose_basis"),                  # $17 dose_basis
                        row.get("frequency"),                   # $18 frequency
                        row.get("duration"),                    # $19 duration
                        str(row.get("max_daily_dose")) if row.get("max_daily_dose") is not None else None,  # $20 max_daily_dose
                        row.get("administration_notes"),        # $21 administration_notes
                        row.get("adjustment_required_for") or [],  # $22 adjustment_required_for
                        row.get("source_section"),              # $23 source_section
                        row.get("source_excerpt"),              # $24 source_excerpt
                    ))

            for record in insert_records:
                result_logger.info(
                    f"EXTRACTED | drug={generic_name} | "
                    f"master_linkage_id={master_linkage_id} | "
                    f"formulation_id={record[1]} | "
                    f"regimen_id={record[0]} | "
                    f"indication={record[2]} | "
                    f"age_group={record[3]} | "
                    f"route={record[12]} | "
                    f"dose_amount={record[13]} | "
                    f"dose_value={record[14]} | "
                    f"dose_unit={record[15]} | "
                    f"frequency={record[17]} | "
                    f"renal_function={record[10]} | "
                    f"hepatic_function={record[11]}"
                )
            result_logger.info(
                f"DRUG_SUMMARY | drug={generic_name} | "
                f"master_linkage_id={master_linkage_id} | "
                f"rows_extracted={len(insert_records)}"
            )

            # Log SUCCESS/PARTIAL status to JSONL
            try:
                with jsonlines.open(RESPONSE_CACHE_PATH, mode='a') as writer:
                    writer.write({
                        "master_linkage_id": str(master_linkage_id),
                        "generic_name": generic_name,
                        "timestamp": datetime.utcnow().isoformat(),
                        "status": "PARTIAL" if is_partial else "SUCCESS",
                        "rows_extracted": len(insert_records)
                    })
            except Exception as je:
                logger.debug(f"JSONL cache write error: {je}")

            if is_partial:
                try:
                    async with pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO drugdb.failed_drugs
                                (master_linkage_id, generic_name, failure_reason)
                            VALUES ($1, $2, 'PARTIAL')
                            ON CONFLICT (master_linkage_id) DO UPDATE
                            SET failure_reason = 'PARTIAL',
                                failed_at = NOW()
                        """, master_linkage_id, generic_name)
                except Exception as fe:
                    logger.warning(f"Parser: failed_drugs PARTIAL insert error: {fe}")

            await write_q.put((master_linkage_id, insert_records))

        except Exception as e:
            logger.error(f"Parser: unexpected error: {e}", exc_info=True)
        finally:
            result_q.task_done()


async def db_writer(
    pool: asyncpg.Pool,
    write_q: asyncio.Queue,
    stats: dict,
):
    """Stage 4: Buffers insert records from write_q and flushes them to
    drugdb.dosing_regimen in batches of WRITE_FLUSH_SIZE rows.

    Saves checkpoint after every flush. Logs detailed progress every
    LOG_EVERY drugs (count, elapsed, rate, ETA, cost). Flushes any
    remaining buffer when the None sentinel is received.
    """
    buffer: list[tuple] = []
    processed_ids: set[str] = set()
    current_offset = 0

    async def flush_buffer() -> None:
        """Write the current buffer to DB, update stats, reset buffer."""
        nonlocal buffer
        if not buffer:
            return
        try:
            async with pool.acquire() as conn:
                await conn.executemany(INSERT_SQL, buffer)
                actual_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM drugdb.dosing_regimen"
                )
            stats["inserted"] = actual_count
            logger.info(
                f"DB Writer: flushed batch of {len(buffer)} rows "
                f"(actual DB total: {stats['inserted']})"
            )
            result_logger.info(
                f"DB_INSERT_SUCCESS | attempted={len(buffer)} | "
                f"actual_db_total={stats['inserted']}"
            )
        except Exception as e:
            logger.error(f"DB Writer: batch insert error: {e}", exc_info=True)
            stats["failed"] += len(buffer)
            result_logger.error(
                f"DB_INSERT_FAILED | rows={len(buffer)} | error={e} | "
                f"RECOVERY: these rows are in the result log and can be reinserted manually"
            )
            for rec in buffer:
                result_logger.error(
                    f"DB_INSERT_FAILED_ROW | regimen_id={rec[0]} | "
                    f"formulation_id={rec[1]} | "
                    f"indication={rec[2]} | "
                    f"age_group={rec[3]} | "
                    f"route={rec[12]} | "
                    f"dose_amount={rec[13]} | "
                    f"dose_value={rec[14]} | "
                    f"dose_unit={rec[15]} | "
                    f"frequency={rec[17]}"
                )
        finally:
            buffer = []

    while True:
        item = await write_q.get()
        try:
            if item is None:
                await flush_buffer()
                save_checkpoint(processed_ids, 0, stats)
                return

            master_linkage_id, records = item
            buffer.extend(records)
            processed_ids.add(str(master_linkage_id))
            stats["processed"] += 1

            if len(buffer) >= WRITE_FLUSH_SIZE:
                await flush_buffer()
                save_checkpoint(processed_ids, 0, stats)

            # Periodic progress log every 10 drugs processed
            if stats["processed"] % 10 == 0:
                elapsed = (datetime.utcnow() - stats["start_time"]).total_seconds()
                rate    = stats["processed"] / elapsed if elapsed > 0 else 0
                eta_m   = ((TOTAL_DRUGS - stats["processed"]) / rate / 60) if rate > 0 else 0
                cost    = stats["processed"] * COST_PER_DRUG
                logger.info(
                    f"Progress | processed: {stats['processed']} | "
                    f"inserted: {stats['inserted']} | "
                    f"elapsed: {elapsed:.0f}s | "
                    f"rate: {rate:.1f} drugs/s | "
                    f"ETA: {eta_m:.1f}m | "
                    f"cost so far: ${cost:.2f}"
                )
                result_logger.info(
                    f"PROGRESS | processed={stats['processed']} | "
                    f"inserted={stats['inserted']} | "
                    f"failed={stats['failed']} | "
                    f"elapsed={elapsed:.1f}s | "
                    f"rate={rate:.3f} drugs/s | "
                    f"cost_so_far=${cost:.4f} | "
                    f"eta_remaining={int(TOTAL_DRUGS - stats['processed'])} drugs | "
                    f"eta_minutes={((TOTAL_DRUGS - stats['processed']) / rate / 60) if rate > 0 else 0:.1f}"
                )
                save_checkpoint(processed_ids, 0, stats)

        except Exception as e:
            logger.error(f"DB Writer: unexpected error: {e}", exc_info=True)
        finally:
            write_q.task_done()

# =============================================================================
# SECTION 9 — Main orchestration
# =============================================================================

async def main(dry_run: bool = False) -> None:
    """Orchestrate all 4 pipeline stages simultaneously via asyncio.gather().

    Creates the DB connection pool, aiohttp session, three async queues,
    and a shared stats dict. Runs producer + 50 api_workers + parser +
    db_writer concurrently. Logs final summary with cost estimate on exit.
    """
    if not DEEPSEEK_API_KEY:
        logger.error("DEEPSEEK_API_KEY is not set. Exiting.")
        sys.exit(1)

    mode  = "DRY RUN" if dry_run else "FULL RUN"
    limit = DRY_RUN_LIMIT if dry_run else None

    logger.info(
        f"=== Dosing Regimen Extraction STARTED === "
        f"Mode: {mode} | {datetime.utcnow().isoformat()} | "
        f"Workers: {MAX_WORKERS} | Batch: {BATCH_SIZE}"
    )
    if dry_run:
        logger.info(f"DRY RUN: will process at most {DRY_RUN_LIMIT} records")

    # --- DB connection pool ---
    pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        min_size=3, max_size=8,
    )

    # --- HTTP session — 120s is safe now that label texts are truncated ---
    timeout = aiohttp.ClientTimeout(total=300)
    session = aiohttp.ClientSession(timeout=timeout)

    # --- Three bounded queues (back-pressure prevents memory blow-up) ---
    fetch_q  = asyncio.Queue(maxsize=200)
    result_q = asyncio.Queue(maxsize=500)
    write_q  = asyncio.Queue(maxsize=1000)

    # --- Shared stats dict (mutated by db_writer) ---
    stats = {
        "processed":  0,
        "inserted":   0,
        "failed":     0,
        "start_time": datetime.utcnow(),
    }

    logger.info(
        f"Launching: 1 producer + {MAX_WORKERS} API workers + "
        f"1 parser + 1 db_writer"
    )

    try:
        workers = [api_worker(session, pool, fetch_q, result_q, i) for i in range(MAX_WORKERS)]
        await asyncio.gather(
            producer(pool, fetch_q, limit=limit),
            *workers,
            parser(pool, result_q, write_q),
            db_writer(pool, write_q, stats),
        )
    finally:
        elapsed = (datetime.utcnow() - stats["start_time"]).total_seconds()
        cost    = stats["processed"] * COST_PER_DRUG
        logger.info(
            f"=== Pipeline COMPLETE ===\n"
            f"  Total processed : {stats['processed']}\n"
            f"  Total inserted  : {stats['inserted']}\n"
            f"  Total failed    : {stats['failed']}\n"
            f"  Elapsed         : {elapsed:.1f}s  ({elapsed / 60:.1f} min)\n"
            f"  Total cost      : ${cost:.4f} USD\n"
            f"  Dead-letter log : {DEAD_LETTER_FILE}"
        )
        result_logger.info(
            f"RUN_COMPLETE | "
            f"processed={stats['processed']} | "
            f"inserted={stats['inserted']} | "
            f"failed={stats['failed']} | "
            f"elapsed_seconds={elapsed:.1f} | "
            f"elapsed_minutes={elapsed/60:.1f} | "
            f"cost_usd=${cost:.4f} | "
            f"avg_seconds_per_drug={elapsed/stats['processed'] if stats['processed'] > 0 else 0:.1f} | "
            f"projected_full_run_hours={((elapsed/stats['processed']) * (TOTAL_DRUGS - stats['processed']) / 3600) if stats['processed'] > 0 else 0:.1f}"
        )
        await pool.close()
        await session.close()

# =============================================================================
# SECTION 10 — Entry point
# =============================================================================

if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="Dosing regimen extraction pipeline")
    arg_parser.add_argument(
        "--dry-run",
        action="store_true",
        help=f"Process only {DRY_RUN_LIMIT} records for testing",
    )
    arg_parser.add_argument(
        "--full-run",
        action="store_true",
        help="Process all 47,481 records",
    )
    args = arg_parser.parse_args()

    if not args.dry_run and not args.full_run:
        print("ERROR: Must specify --dry-run or --full-run")
        print("  python dosing_regimen_extraction.py --dry-run   # test 10 records")
        print("  python dosing_regimen_extraction.py --full-run  # all 47,481 records")
        sys.exit(1)

    if not os.getenv("DEEPSEEK_API_KEY"):
        print("ERROR: DEEPSEEK_API_KEY environment variable not set")
        print("  export DEEPSEEK_API_KEY=your_key_here")
        sys.exit(1)

    asyncio.run(main(dry_run=args.dry_run))
