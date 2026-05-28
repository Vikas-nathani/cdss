#!/usr/bin/env python3
"""
test_severity_classification.py

Standalone test — verifies that Qwen 2.5 7B Instruct correctly classifies
severity and extracts mechanism from real drug interaction descriptions BEFORE
committing to the full enrich_severity_mechanism.py pipeline run.

Uses RunPod serverless vLLM with 5 parallel async workers.
By default READ-ONLY. Pass --write-to-db to persist results.

How to run:
    python3 scripts/test_severity_classification.py \
        --db-password YOUR_DB_PASSWORD

With custom sample size:
    python3 scripts/test_severity_classification.py \
        --db-password YOUR_DB_PASSWORD \
        --samples 25

Workers: 5 parallel (hardcoded, controlled by semaphore)
"""

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Dependency guards — fail fast with clear instructions ─────────────────────
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

try:
    from openai import AsyncOpenAI
except ImportError:
    print("ERROR: openai not installed. Run: pip install openai")
    sys.exit(1)

try:
    import colorama
    colorama.init(autoreset=True)
    GREEN  = colorama.Fore.GREEN
    RED    = colorama.Fore.RED
    YELLOW = colorama.Fore.YELLOW
    CYAN   = colorama.Fore.CYAN
    BOLD   = colorama.Style.BRIGHT
    RESET  = colorama.Style.RESET_ALL
except ImportError:
    GREEN = RED = YELLOW = CYAN = BOLD = RESET = ""

# =============================================================================
# CONSTANTS
# =============================================================================

RUNPOD_API_KEY  = os.environ.get("RUNPOD_API_KEY", "")
RUNPOD_BASE_URL = "https://api.runpod.ai/v2/fahewj4m3wv52x/openai/v1"
MODEL_NAME      = "Qwen/Qwen2.5-7B-Instruct"
NUM_WORKERS     = 5

FULL_RUN_RECORDS = 1_455_278   # unique A→B pairs to classify
MIN_SAMPLES      = 20
MAX_SAMPLES      = 30

SEVERITY_VALUES = frozenset({"contraindicated", "major", "moderate", "minor", "unknown"})

# Identical to enrich_severity_mechanism.py — must never diverge
SYSTEM_PROMPT = (
    "You are a clinical pharmacologist. Classify drug interactions.\n"
    "Respond ONLY with valid JSON. No explanation. No markdown.\n"
    'Format: {"severity": "<value>", "mechanism": "<short phrase>"}\n'
    "Severity must be exactly one of: contraindicated, major, moderate, minor, unknown\n"
    "Mechanism: extract the pharmacological mechanism in 3-8 words.\n"
    'If unclear use: {"severity": "unknown", "mechanism": null}'
)

# DISTINCT ON dedups by description; shuffle done in Python after fetch.
SAMPLE_SQL = """
    SELECT DISTINCT ON (ii.description)
        ii.id::text           AS row_id,
        ii.reacting_id::text  AS reacting_id,
        subj.name             AS subject_name,
        react.name            AS partner_name,
        ii.description
    FROM drugdb.ingredient_interactions ii
    JOIN drugdb.ingredients subj  ON subj.id  = ii.id
    JOIN drugdb.ingredients react ON react.id = ii.reacting_id
    WHERE LENGTH(ii.description) > 100
      AND subj.name IS NOT NULL
      AND react.name IS NOT NULL
    ORDER BY ii.description, ii.id
    LIMIT %s
"""

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(log_file: str) -> logging.Logger:
    """Configure dual logging to stdout and log_file simultaneously."""
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    fmt    = "%(asctime)s | %(levelname)s | %(message)s"
    logger = logging.getLogger("test_severity")
    logger.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter(fmt))
    logger.addHandler(ch)

    fh = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt))
    logger.addHandler(fh)

    return logger

# =============================================================================
# DATABASE
# =============================================================================

def fetch_samples(args: argparse.Namespace, logger: logging.Logger) -> list[dict]:
    """Connect to PostgreSQL, fetch N diverse drug interaction samples, then shuffle."""
    logger.info(
        f"Connecting to {args.db_host}:{args.db_port}/{args.db_name} as {args.db_user}"
    )
    try:
        conn = psycopg2.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password,
        )
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SAMPLE_SQL, (args.samples,))
            rows = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.error(f"Database connection/query failed: {e}")
        sys.exit(1)

    records = [dict(r) for r in rows]
    random.shuffle(records)
    logger.info(f"Fetched {len(records)} sample records from drugdb.ingredient_interactions")
    return records

# =============================================================================
# DATABASE WRITE
# =============================================================================

UPDATE_SQL = """
    UPDATE drugdb.ingredient_interactions
    SET severity  = %s,
        mechanism = %s
    WHERE id          = %s::uuid
    AND   reacting_id = %s::uuid
"""


def write_results_to_db(
    conn,
    results: list,
    records: list,
    log: logging.Logger,
) -> int:
    """Write severity and mechanism results back to drugdb.ingredient_interactions.
    Only writes rows where status == 'SUCCESS'. Returns number of rows written.
    """
    log.info("Writing results to database...")

    updates = []
    for result, record in zip(results, records):
        if result.get("status") == "SUCCESS":
            updates.append((
                result["severity"],
                result["mechanism"],
                record["row_id"],
                record["reacting_id"],
            ))

    if not updates:
        log.warning("No successful results to write to DB")
        return 0

    try:
        with conn.cursor() as cur:
            cur.executemany(UPDATE_SQL, updates)
        conn.commit()
        log.info(
            "Written %d rows to drugdb.ingredient_interactions (severity + mechanism)",
            len(updates),
        )
        return len(updates)
    except Exception as exc:
        conn.rollback()
        log.error("DB write failed: %s", exc, exc_info=True)
        raise


# =============================================================================
# PER-RECORD DISPLAY
# =============================================================================

def print_record_result(
    index, total,
    subject, partner, description,
    severity, mechanism, raw_response,
    elapsed, status,
    log=None,
):
    status_icon = "✓" if status == "SUCCESS" else "✗"
    print(f"\n{'─'*54}")
    print(f"Record {index} of {total}")
    print(f"{'─'*54}")
    print(f"Drug A (subject) : {subject}")
    print(f"Drug B (partner) : {partner}")
    print(f"Description      : {description[:200]}")
    print(f"\nLLM Response:")
    if status == "SUCCESS":
        print(f"  severity       : {severity}")
        print(f"  mechanism      : {mechanism}")
    print(f"\nParse status     : {status_icon} {status}")
    if elapsed > 0:
        print(f"Response time    : {elapsed:.2f}s")
    print(f"Raw response     : {raw_response}")
    sys.stdout.flush()

    if log is not None:
        log.info(
            "RECORD %d/%d | subject=%r | partner=%r | status=%s | "
            "severity=%s | mechanism=%s | elapsed=%.2fs | "
            "desc_preview=%r | raw=%r",
            index, total, subject, partner, status,
            severity, mechanism, elapsed,
            description[:100], str(raw_response)[:120],
        )

# =============================================================================
# ASYNC CLASSIFICATION
# =============================================================================

async def classify_single(
    client: AsyncOpenAI,
    semaphore: asyncio.Semaphore,
    record: dict,
    index: int,
    total: int,
    log,
) -> dict:
    """Classify one record with semaphore to limit concurrency."""
    async with semaphore:
        try:
            t0 = asyncio.get_event_loop().time()
            response = await client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"A:{record['subject_name']} B:{record['partner_name']}\n{record['description']}"}
                ],
                max_tokens=60,
                temperature=0
            )
            elapsed = asyncio.get_event_loop().time() - t0
            content = response.choices[0].message.content
            parsed  = json.loads(content)
            severity  = parsed.get("severity", "unknown")
            mechanism = parsed.get("mechanism", None)

            if severity not in SEVERITY_VALUES:
                severity = "unknown"

            print_record_result(
                index, total,
                record['subject_name'], record['partner_name'],
                record['description'], severity, mechanism,
                content, elapsed, "SUCCESS",
                log=log,
            )
            return {
                "status":    "SUCCESS",
                "severity":  severity,
                "mechanism": mechanism,
                "elapsed":   elapsed,
            }

        except json.JSONDecodeError as exc:
            log.warning("Record %d: JSON parse failed: %s", index, exc)
            raw = response.choices[0].message.content if 'response' in dir() else "NO RESPONSE"
            print_record_result(
                index, total,
                record['subject_name'], record['partner_name'],
                record['description'], None, None,
                raw, 0, "PARSE_FAILED",
                log=log,
            )
            return {"status": "parse_failed", "severity": "unknown", "mechanism": None, "elapsed": 0}

        except Exception as exc:
            log.error("Record %d: API call failed: %s", index, exc)
            print_record_result(
                index, total,
                record['subject_name'], record['partner_name'],
                record['description'], None, None,
                str(exc), 0, "API_FAILED",
                log=log,
            )
            return {"status": "api_failed", "severity": "unknown", "mechanism": None, "elapsed": 0}


async def run_classification(records: list, log) -> list:
    """Run all records in parallel with 5 workers."""
    client    = AsyncOpenAI(
        api_key=RUNPOD_API_KEY,
        base_url=RUNPOD_BASE_URL
    )
    semaphore = asyncio.Semaphore(NUM_WORKERS)
    total     = len(records)

    log.info("Running %d records with %d parallel workers...", total, NUM_WORKERS)

    tasks = [
        classify_single(client, semaphore, record, i + 1, total, log)
        for i, record in enumerate(records)
    ]
    results = await asyncio.gather(*tasks)
    await client.close()
    return list(results)

# =============================================================================
# SUMMARY DISPLAY
# =============================================================================

def _pct(count: int, total: int) -> str:
    p = count / total * 100 if total > 0 else 0.0
    return f"{count:>3}  ({p:.1f}%)"


def print_summary(
    records: list[dict],
    results: list[dict],
    total_elapsed: float,
    logger: logging.Logger,
    db_write_info: str = "NO  - pass --write-to-db to persist results",
) -> None:
    """Print the final test summary table and VERDICT line."""
    n = len(results)
    if n == 0:
        msg = "No results to summarize."
        print(msg)
        logger.warning(msg)
        return

    n_success  = sum(1 for r in results if r["status"] == "SUCCESS")
    n_failed   = sum(1 for r in results if r["status"] == "parse_failed")
    n_api_fail = sum(1 for r in results if r["status"] == "api_failed")

    sev_order = ["contraindicated", "major", "moderate", "minor", "unknown"]
    sev_dist  = {s: 0 for s in sev_order}
    for r in results:
        sev = r.get("severity", "")
        if sev in SEVERITY_VALUES:
            sev_dist[sev] += 1

    times    = [r["elapsed"] for r in results if r.get("elapsed", 0) > 0]
    avg_time = sum(times) / len(times) if times else 0.0
    speedup  = (avg_time * n) / total_elapsed if total_elapsed > 0 else 0.0

    success_pct = (n_success / n * 100) if n > 0 else 0.0
    is_ready    = success_pct >= 90.0
    verdict     = "READY FOR FULL RUN" if is_ready else "NEEDS PROMPT ADJUSTMENT"

    DIV = "═" * 54

    lines = [
        "",
        DIV,
        "TEST SUMMARY — RunPod vLLM + Qwen 2.5 7B Instruct",
        DIV,
        f"Provider        : RunPod Serverless vLLM",
        f"Model           : {MODEL_NAME}",
        f"Endpoint        : {RUNPOD_BASE_URL}",
        f"Parallel workers: {NUM_WORKERS}",
        f"Records tested  : {n}",
        "",
        "Results:",
        f"  Parsed successfully : {_pct(n_success,  n)}",
        f"  JSON parse failed   : {_pct(n_failed,   n)}",
        f"  API call failed     : {_pct(n_api_fail, n)}",
        "",
        "Severity Distribution:",
    ]
    for sev in sev_order:
        lines.append(f"  {sev:<22}: {sev_dist[sev]}")
    lines += [
        "",
        "Performance:",
        f"  Total wall clock time : {total_elapsed:.1f}s for {n} records",
        f"  Avg response time     : {avg_time:.2f}s per record",
        f"  Estimated speedup     : {speedup:.1f}x vs sequential  (5x theoretical)",
        "",
        "Cost (this test):",
        f"  RunPod serverless : ~$0.00-0.05 (GPU seconds, {NUM_WORKERS} workers)",
        f"  Note: Cost depends on cold start + inference time per worker",
        "",
        f"  Model verdict     : {'GOOD' if is_ready else 'NEEDS REVIEW'}",
        "",
        f"DB write         : {db_write_info}",
    ]

    failures = [
        (i + 1, records[i], results[i])
        for i in range(n)
        if results[i]["status"] in ("parse_failed", "api_failed")
    ]
    if failures:
        lines.append("")
        lines.append("Sample failed records (if any):")
        for idx, rec, res in failures[:5]:
            s = rec.get("subject_name", "?")
            p = rec.get("partner_name", "?")
            lines.append(f"  Record {idx}: {res['status']} | {s} ↔ {p}")

    lines += [
        "",
        DIV,
        f"VERDICT: {verdict}",
        DIV,
    ]

    # Print colored version to console
    print(f"\n{BOLD}{DIV}{RESET}")
    print(f"{BOLD}TEST SUMMARY — RunPod vLLM + Qwen 2.5 7B Instruct{RESET}")
    print(f"{BOLD}{DIV}{RESET}")
    print(f"Provider        : RunPod Serverless vLLM")
    print(f"Model           : {MODEL_NAME}")
    print(f"Endpoint        : {RUNPOD_BASE_URL}")
    print(f"Parallel workers: {NUM_WORKERS}")
    print(f"Records tested  : {n}")
    print()
    print("Results:")
    print(f"  {GREEN}✓{RESET} Parsed successfully : {_pct(n_success,  n)}")
    print(f"  {RED}✗{RESET} JSON parse failed   : {_pct(n_failed,   n)}")
    print(f"  {RED}✗{RESET} API call failed     : {_pct(n_api_fail, n)}")
    print()
    print("Severity Distribution:")
    for sev in sev_order:
        print(f"  {sev:<22}: {sev_dist[sev]}")
    print()
    print("Performance:")
    print(f"  Total wall clock time : {total_elapsed:.1f}s for {n} records")
    print(f"  Avg response time     : {avg_time:.2f}s per record")
    print(f"  Estimated speedup     : {speedup:.1f}x vs sequential  (5x theoretical)")
    print()
    print("Cost (this test):")
    print(f"  RunPod serverless : ~$0.00-0.05 (GPU seconds, {NUM_WORKERS} workers)")
    print(f"  Note: Cost depends on cold start + inference time per worker")
    print()
    print(f"  Model verdict     : {'GOOD' if is_ready else 'NEEDS REVIEW'}")
    print()
    print(f"DB write         : {db_write_info}")
    if failures:
        print()
        print("Sample failed records (if any):")
        for idx, rec, res in failures[:5]:
            s = rec.get("subject_name", "?")
            p = rec.get("partner_name", "?")
            print(f"  Record {idx}: {res['status']} | {s} ↔ {p}")
    print()
    print(f"{BOLD}{DIV}{RESET}")
    if is_ready:
        print(f"{GREEN}{BOLD}VERDICT: READY FOR FULL RUN{RESET}")
    else:
        print(f"{YELLOW}{BOLD}VERDICT: NEEDS PROMPT ADJUSTMENT{RESET}")
    print(f"{BOLD}{DIV}{RESET}")
    sys.stdout.flush()

    # Write full plain-text summary to log file
    for line in lines:
        logger.info(line)

    logger.info(
        "SUMMARY: %d/%d success (%.1f%%) | wall_time=%.1fs | avg_time=%.2fs | "
        "speedup=%.1fx | verdict=%s",
        n_success, n, success_pct, total_elapsed, avg_time, speedup,
        "READY" if is_ready else "NEEDS_REVIEW",
    )

# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    ts          = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    default_log = f"logs/test_severity_classification_{ts}.log"

    parser = argparse.ArgumentParser(
        description=(
            "Standalone test — verifies Qwen 2.5 7B Instruct severity/mechanism "
            "classification against real drug interaction records via RunPod vLLM."
        )
    )
    parser.add_argument(
        "--db-password", required=True,
        help="PostgreSQL database password",
    )
    parser.add_argument(
        "--db-host", default=os.environ.get("DB_HOST", "localhost"),
        help="Database host (default: localhost)",
    )
    parser.add_argument(
        "--db-port", type=int, default=5432,
        help="Database port (default: 5432)",
    )
    parser.add_argument(
        "--db-name", default="postgres",
        help="Database name (default: postgres)",
    )
    parser.add_argument(
        "--db-user", default="postgres",
        help="Database user (default: postgres)",
    )
    parser.add_argument(
        "--samples", type=int, default=25,
        help="Number of records to test (default: 25, min: 20, max: 30)",
    )
    parser.add_argument(
        "--log-file", default=default_log,
        help=f"Log file path (default: {default_log})",
    )
    parser.add_argument(
        "--write-to-db",
        action="store_true",
        help="Write classified severity and mechanism back to drugdb.ingredient_interactions",
    )
    args = parser.parse_args()

    if args.samples < 20 or args.samples > 30:
        print("ERROR: --samples must be between 20 and 30")
        print(f"       You provided: {args.samples}")
        print("       Usage: --samples 25")
        sys.exit(1)

    logger = setup_logging(args.log_file)
    logger.info(
        "=== Severity Classification Test STARTED === "
        "%s | model=%s | samples=%d | workers=%d | log=%s",
        datetime.utcnow().isoformat(), MODEL_NAME, args.samples, NUM_WORKERS, args.log_file,
    )

    records = fetch_samples(args, logger)
    if not records:
        logger.error(
            "No records returned. Possible causes: no 'unknown' severity rows, "
            "or descriptions are all too short (< 100 chars)."
        )
        sys.exit(1)

    n = len(records)
    logger.info("Starting async API test on %d records with %d workers", n, NUM_WORKERS)

    run_start     = time.time()
    results       = asyncio.run(run_classification(records, logger))
    total_elapsed = time.time() - run_start

    db_write_info = "NO  - pass --write-to-db to persist results"
    if args.write_to_db:
        try:
            conn = psycopg2.connect(
                host=args.db_host,
                port=args.db_port,
                dbname=args.db_name,
                user=args.db_user,
                password=args.db_password,
            )
            rows_written = write_results_to_db(conn, results, records, logger)
            conn.close()
            db_write_info = (
                f"YES - {rows_written} rows updated in drugdb.ingredient_interactions"
            )
        except Exception as exc:
            logger.error("DB write step failed: %s", exc)
            db_write_info = f"FAILED - {exc}"

    print_summary(records, results, total_elapsed, logger, db_write_info)
    logger.info("=== Severity Classification Test COMPLETE ===")


if __name__ == "__main__":
    main()
