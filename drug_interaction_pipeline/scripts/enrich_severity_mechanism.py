#!/usr/bin/env python3
"""
enrich_severity_mechanism.py

Enriches drugdb.ingredient_interactions with severity + mechanism
using OpenRouter API + Qwen2.5-7B-Instruct via fully async
producer-consumer pipeline.

Pipeline stages:
  Stage 1: Pre-filter by regex SQL (free, no LLM)
  Stage 2: Async producer-consumer LLM classification
           - Producer streams unique descriptions from DB
           - 100 async workers consume from queue
           - Each worker calls OpenRouter directly
           - Results written to DB immediately per worker
           - No batch files, no polling, no intermediate storage
  Stage 3: SQL mirror A->B results to B->A rows
  Stage 4: Final verification + summary

How to run:
  # Dry run first (no API calls, no DB writes)
  python3 scripts/enrich_severity_mechanism.py \\
      --openrouter-api-key YOUR_KEY \\
      --db-password YOUR_PASSWORD \\
      --dry-run

  # Full run
  nohup python3 scripts/enrich_severity_mechanism.py \\
      --openrouter-api-key YOUR_KEY \\
      --db-password YOUR_PASSWORD \\
      --workers 100 \\
      --log-file logs/enrich_severity_mechanism.log \\
      > logs/enrich_nohup.log 2>&1 &
  echo "PID: $!"

  # Monitor
  tail -f logs/enrich_severity_mechanism.log

  # Resume if interrupted
  python3 scripts/enrich_severity_mechanism.py \\
      --openrouter-api-key YOUR_KEY \\
      --db-password YOUR_PASSWORD \\
      --resume \\
      --checkpoint logs/severity_checkpoint.json
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, Optional

import psycopg2
import psycopg2.pool

try:
    from openai import AsyncOpenAI
except ImportError:
    AsyncOpenAI = None


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────

TOTAL_ROWS      = 2_910_556
TOTAL_UNIQUE    = 1_455_278

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
MODEL               = "qwen/qwen-2.5-7b-instruct"
OPENROUTER_HEADERS  = {
    "HTTP-Referer": "https://cdss.app",
    "X-Title": "CDSS Drug Interaction Enrichment"
}

SEVERITY_VALUES = frozenset({
    "contraindicated", "major", "moderate", "minor", "unknown"
})

# OpenRouter Qwen2.5-7B pricing
INPUT_PRICE_PER_1M  = 0.04   # $0.04 per 1M input tokens
OUTPUT_PRICE_PER_1M = 0.10   # $0.10 per 1M output tokens
# 5.5% OpenRouter platform fee added on top
PLATFORM_FEE        = 0.055

AVG_INPUT_TOKENS    = 150
AVG_OUTPUT_TOKENS   = 40

QUEUE_MAXSIZE       = 2000   # bounded queue — prevents DB outpacing workers
WORKER_FLUSH_SIZE   = 500    # each worker flushes DB every 500 records
PROGRESS_INTERVAL   = 10_000 # log progress every N classified rows
CHECKPOINT_INTERVAL = 10_000 # save checkpoint every N classified rows

SYSTEM_PROMPT = (
    "You are a clinical pharmacologist. Classify drug interactions.\n"
    "Respond ONLY with valid JSON. No explanation. No markdown.\n"
    'Format: {"severity": "<value>", "mechanism": "<short phrase>"}\n'
    "Severity must be exactly one of: "
    "contraindicated, major, moderate, minor, unknown\n"
    "Mechanism: extract the pharmacological mechanism in 3-8 words.\n"
    'If unclear use: {"severity": "unknown", "mechanism": null}'
)

PREFILTER_CONDITIONS = [
    ("contraindicated", "description ILIKE '%contraindicated%'"),
    (
        "major",
        "(description ILIKE '%life-threatening%'"
        " OR description ILIKE '%serious adverse%'"
        " OR description ILIKE '%fatal%'"
        " OR description ILIKE '%severe%')",
    ),
    (
        "minor",
        "(description ILIKE '%minor%'"
        " AND NOT description ILIKE '%major%'"
        " AND NOT description ILIKE '%severe%')",
    ),
]

FETCH_UNIQUE_SQL = """
    SELECT DISTINCT ON (ii.description)
        ii.id::text,
        ii.reacting_id::text,
        ii.description
    FROM drugdb.ingredient_interactions ii
    WHERE ii.severity = 'unknown'
    ORDER BY ii.description, ii.id
"""

MIRROR_SQL = """
    UPDATE drugdb.ingredient_interactions AS target
    SET
        severity  = source.severity,
        mechanism = source.mechanism
    FROM drugdb.ingredient_interactions AS source
    WHERE target.id          = source.reacting_id
      AND target.reacting_id = source.id
      AND source.severity   != 'unknown'
      AND target.severity    = 'unknown'
"""

VERIFY_SQL = """
    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE severity = 'unknown'),
        COUNT(*) FILTER (WHERE severity = 'contraindicated'),
        COUNT(*) FILTER (WHERE severity = 'major'),
        COUNT(*) FILTER (WHERE severity = 'moderate'),
        COUNT(*) FILTER (WHERE severity = 'minor'),
        COUNT(*) FILTER (WHERE mechanism IS NOT NULL),
        COUNT(*) FILTER (WHERE mechanism IS NULL)
    FROM drugdb.ingredient_interactions
"""

UPDATE_SQL = """
    UPDATE drugdb.ingredient_interactions
    SET severity = %s, mechanism = %s
    WHERE id = %s::uuid AND reacting_id = %s::uuid
"""


# ──────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────

@dataclass
class Stats:
    prefilter_contraindicated: int = 0
    prefilter_major:           int = 0
    prefilter_minor:           int = 0
    unique_to_process:         int = 0
    rows_classified:           int = 0
    rows_failed:               int = 0
    parse_errors:              int = 0
    rows_mirrored:             int = 0
    total_input_tokens:        int = 0
    total_output_tokens:       int = 0
    flush_errors:              int = 0
    start_time: float = field(default_factory=time.monotonic)
    _lock: Optional[asyncio.Lock] = field(default=None, init=False, repr=False)

    async def increment(self, **kwargs):
        # Lazy lock creation — safe inside running event loop
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Enrich ingredient_interactions with severity + mechanism via OpenRouter"
    )
    p.add_argument("--openrouter-api-key", required=True,
                   help="OpenRouter API key")
    p.add_argument("--db-password",        required=True,
                   help="PostgreSQL password")
    p.add_argument("--db-host",    default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--db-port",    type=int, default=5432)
    p.add_argument("--db-name",    default="postgres")
    p.add_argument("--db-user",    default="postgres")
    p.add_argument("--workers",    type=int, default=100,
                   help="Parallel async workers (default: 100)")
    p.add_argument("--dry-run",    action="store_true",
                   help="Show 10 sample records + estimates; no API calls, no DB writes")
    p.add_argument("--skip-prefilter", action="store_true",
                   help="Skip Stage 1 regex pre-filter")
    p.add_argument("--skip-mirror",    action="store_true",
                   help="Skip Stage 3 SQL mirror step")
    p.add_argument("--max-retries",    type=int, default=5,
                   help="Max retries per API call (default: 5)")
    p.add_argument("--log-file",
                   default="logs/enrich_severity_mechanism.log")
    p.add_argument("--checkpoint",
                   default="logs/severity_checkpoint.json")
    p.add_argument("--resume",     action="store_true",
                   help="Resume from existing checkpoint file")
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of records processed in Stage 2 "
             "(for testing only — omit for full run)"
    )
    return p.parse_args()


# ──────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────

def setup_logging(log_file: str) -> logging.Logger:
    log = logging.getLogger("severity_enrich")
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    try:
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        log.addHandler(fh)
    except OSError as exc:
        log.warning("Cannot open log file %s: %s — console only", log_file, exc)

    return log


# ──────────────────────────────────────────────────────────────
# DB connection
# ──────────────────────────────────────────────────────────────

def get_db_connection(
    args: argparse.Namespace,
    max_retries: int = 3,
    log: Optional[logging.Logger] = None,
) -> psycopg2.extensions.connection:
    for attempt in range(1, max_retries + 1):
        try:
            return psycopg2.connect(
                host=args.db_host, port=args.db_port,
                dbname=args.db_name, user=args.db_user,
                password=args.db_password, connect_timeout=30,
            )
        except psycopg2.OperationalError as exc:
            if log:
                log.error("DB connection failed (attempt %d/%d): %s", attempt, max_retries, exc)
            if attempt >= max_retries:
                raise
            time.sleep(30)


# ──────────────────────────────────────────────────────────────
# DB Connection Pool
# ──────────────────────────────────────────────────────────────

def create_connection_pool(
    args: argparse.Namespace,
    size: int,
) -> psycopg2.pool.ThreadedConnectionPool:
    return psycopg2.pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=size + 5,
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        connect_timeout=30,
    )


# ──────────────────────────────────────────────────────────────
# Checkpoint helpers
# ──────────────────────────────────────────────────────────────

def save_checkpoint(checkpoint: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(checkpoint, f, indent=2, default=str)
    os.replace(tmp, path)


def load_checkpoint(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


# ──────────────────────────────────────────────────────────────
# Preload ingredient names
# ──────────────────────────────────────────────────────────────

def load_ingredient_names(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
) -> Dict[str, str]:
    log.info("Preloading ingredient names into memory …")
    t0 = time.monotonic()
    names: Dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id::text, name FROM drugdb.ingredients WHERE name IS NOT NULL")
        for ing_id, name in cur:
            names[ing_id] = name
    log.info("Loaded %d ingredient names in %.2fs", len(names), time.monotonic() - t0)
    return names


# ──────────────────────────────────────────────────────────────
# Column check
# ──────────────────────────────────────────────────────────────

def check_required_columns(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'drugdb'
              AND table_name   = 'ingredient_interactions'
              AND column_name  IN ('severity', 'mechanism')
        """)
        found = {row[0] for row in cur.fetchall()}
    missing = {"severity", "mechanism"} - found
    if missing:
        log.error(
            "Required column(s) missing from drugdb.ingredient_interactions: %s\n"
            "Run: psql -f schemas/alter_ingredient_interactions_severity.sql",
            ", ".join(sorted(missing)),
        )
        sys.exit(1)
    log.info("Column check passed: severity and mechanism columns exist")


# ──────────────────────────────────────────────────────────────
# Stage 1 — Pre-filter
# ──────────────────────────────────────────────────────────────

def stage1_prefilter(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
    stats: Stats,
    dry_run: bool,
) -> None:
    log.info("=== Stage 1: Pre-filter by regex SQL ===")

    for severity_label, condition in PREFILTER_CONDITIONS:
        count_sql  = f"SELECT COUNT(*) FROM drugdb.ingredient_interactions WHERE severity = 'unknown' AND {condition}"
        update_sql = f"UPDATE drugdb.ingredient_interactions SET severity = '{severity_label}' WHERE severity = 'unknown' AND {condition}"

        if dry_run:
            with conn.cursor() as cur:
                cur.execute(count_sql)
                n = cur.fetchone()[0]
            log.info("  DRY-RUN: would classify %s rows as '%s'", f"{n:,}", severity_label)
        else:
            try:
                with conn.cursor() as cur:
                    cur.execute(update_sql)
                    n = cur.rowcount
                conn.commit()
                log.info("  Classified %s rows as '%s'", f"{n:,}", severity_label)
            except Exception as exc:
                conn.rollback()
                log.error("  Pre-filter UPDATE failed for '%s': %s", severity_label, exc)
                n = 0

            if severity_label == "contraindicated":
                stats.prefilter_contraindicated = n
            elif severity_label == "major":
                stats.prefilter_major = n
            elif severity_label == "minor":
                stats.prefilter_minor = n

    total = stats.prefilter_contraindicated + stats.prefilter_major + stats.prefilter_minor
    if not dry_run:
        log.info(
            "Stage 1 complete: %s rows pre-filtered (%.1f%% of %s total)",
            f"{total:,}", 100 * total / TOTAL_ROWS, f"{TOTAL_ROWS:,}",
        )


# ──────────────────────────────────────────────────────────────
# Stage 2 — Async Producer-Consumer Classification
# ──────────────────────────────────────────────────────────────

async def stage2_async_classify(
    args: argparse.Namespace,
    conn: psycopg2.extensions.connection,
    pool: psycopg2.pool.ThreadedConnectionPool,
    ing_names: Dict[str, str],
    stats: Stats,
    log: logging.Logger,
    checkpoint: dict,
    dry_run: bool = False,
) -> None:
    log.info("=== Stage 2: Async Producer-Consumer Classification ===")

    # Count unique records to process
    with conn.cursor() as cur:
        if args.limit:
            cur.execute(
                "SELECT LEAST(COUNT(DISTINCT description), %s) "
                "FROM drugdb.ingredient_interactions "
                "WHERE severity = 'unknown'",
                (args.limit,)
            )
        else:
            cur.execute(
                "SELECT COUNT(DISTINCT description) "
                "FROM drugdb.ingredient_interactions "
                "WHERE severity = 'unknown'"
            )
        unique_count = cur.fetchone()[0]

    stats.unique_to_process = unique_count
    log.info(
        "Unique descriptions to classify: %s | Workers: %d",
        f"{unique_count:,}", args.workers
    )

    if dry_run:
        # Show 25 samples and estimates only
        with conn.cursor() as cur:
            cur.execute(FETCH_UNIQUE_SQL + " LIMIT 25")
            rows = cur.fetchall()

        log.info("DRY-RUN: 25 sample records that would be sent to LLM:")
        for i, (row_id, reacting_id, desc) in enumerate(rows, 1):
            subject = ing_names.get(row_id, "(unknown)")
            partner = ing_names.get(reacting_id, "(unknown)")
            log.info(
                "\n  ─── Record %d of 25 ───\n"
                "  subject (Drug A) : %s\n"
                "  partner (Drug B) : %s\n"
                "  row_id           : %s\n"
                "  reacting_id      : %s\n"
                "  description      : %s",
                i, subject, partner, row_id, reacting_id, desc
            )

        # Print cost and time estimates
        cost_input  = unique_count * AVG_INPUT_TOKENS  * INPUT_PRICE_PER_1M  / 1_000_000
        cost_output = unique_count * AVG_OUTPUT_TOKENS * OUTPUT_PRICE_PER_1M / 1_000_000
        cost_total  = (cost_input + cost_output) * (1 + PLATFORM_FEE)
        rate        = args.workers  # ~1 record/sec per worker
        eta_hours   = unique_count / rate / 3600

        log.info(
            "\n  ─── DRY-RUN Estimates ───\n"
            "  Total rows in DB          : %s\n"
            "  Unique to LLM             : %s\n"
            "  Workers                   : %d\n"
            "  Est. throughput           : ~%d records/sec\n"
            "  Est. time                 : ~%.1f hours\n"
            "  Est. input tokens         : ~%s\n"
            "  Est. output tokens        : ~%s\n"
            "  Est. cost (with 5.5%% fee): ~$%.2f\n"
            "  Provider                  : OpenRouter\n"
            "  Model                     : %s",
            f"{TOTAL_ROWS:,}",
            f"{unique_count:,}",
            args.workers,
            args.workers,
            eta_hours,
            f"{unique_count * AVG_INPUT_TOKENS:,}",
            f"{unique_count * AVG_OUTPUT_TOKENS:,}",
            cost_total,
            MODEL
        )
        log.info("DRY RUN COMPLETE — remove --dry-run to execute")
        return

    # FULL RUN
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    # ── Producer ──────────────────────────────────────────────
    async def producer():
        """
        Streams rows from DB server-side cursor directly into the queue.
        Never loads all rows into memory — streams row by row.
        Backpressure: blocks when queue is full (maxsize=2000)
        so DB never outpaces workers.
        Sends None poison pills at end to stop each worker.
        """
        loop = asyncio.get_event_loop()

        def _stream():
            src_cur = conn.cursor(name="severity_stream_v2")
            src_cur.itersize = 10_000

            # Apply limit for test runs
            sql = FETCH_UNIQUE_SQL
            if args.limit:
                sql = FETCH_UNIQUE_SQL + f" LIMIT {args.limit}"
                log.info(
                    "Producer: LIMIT %d applied — test run only",
                    args.limit
                )

            src_cur.execute(sql)
            log.info(
                "Producer: fetching unclassified records "
                "(WHERE severity = 'unknown' auto-skips already done rows)"
            )

            count = 0
            for row in src_cur:
                # Put row into async queue from sync thread;
                # blocks when queue full — natural backpressure
                future = asyncio.run_coroutine_threadsafe(
                    queue.put(row), loop
                )
                future.result()  # wait until queue accepts the row
                count += 1
                if count % 50_000 == 0:
                    log.info(
                        "Producer: streamed %s records so far",
                        f"{count:,}"
                    )

            # Send poison pill for each worker
            for _ in range(args.workers):
                future = asyncio.run_coroutine_threadsafe(
                    queue.put(None), loop
                )
                future.result()

            src_cur.close()
            log.info(
                "Producer done — %s total records streamed into queue",
                f"{count:,}"
            )
            return count

        total_streamed = await asyncio.to_thread(_stream)
        log.info("Producer task complete: %s records", f"{total_streamed:,}")

    # ── Single API call with retry ─────────────────────────────
    async def call_openrouter(client, subject, partner, description):
        """
        Calls OpenRouter with exponential backoff on rate limits.
        Returns (severity, mechanism) tuple.
        Never raises — always returns a result even if unknown.
        """
        for attempt in range(args.max_retries):
            try:
                response = await client.chat.completions.create(
                    model=MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": f"A:{subject} B:{partner}\n{description}"
                        }
                    ],
                    max_tokens=60,
                    temperature=0
                    # timeout is set on the AsyncOpenAI client itself (timeout=30.0)
                    # not on individual create() calls
                )
                content   = response.choices[0].message.content.strip()
                parsed    = json.loads(content)
                severity  = parsed.get("severity", "unknown")
                mechanism = parsed.get("mechanism") or None

                if severity not in SEVERITY_VALUES:
                    severity = "unknown"

                # Track token usage
                if hasattr(response, "usage") and response.usage:
                    await stats.increment(
                        total_input_tokens=response.usage.prompt_tokens or 0,
                        total_output_tokens=response.usage.completion_tokens or 0
                    )

                return severity, mechanism

            except json.JSONDecodeError:
                # Model returned non-JSON — mark unknown, no retry
                await stats.increment(parse_errors=1)
                return "unknown", None

            except Exception as exc:
                err_str = str(exc)
                is_rate_limit = "429" in err_str or "rate" in err_str.lower()
                is_last_attempt = attempt >= args.max_retries - 1

                if is_last_attempt:
                    log.warning("API failed after %d attempts: %s", args.max_retries, exc)
                    await stats.increment(rows_failed=1)
                    return "unknown", None

                wait = (2 ** attempt) if is_rate_limit else 5
                await asyncio.sleep(wait)

        return "unknown", None

    # ── DB flush helper ────────────────────────────────────────
    async def flush_to_db(updates: list):
        """
        Borrows a connection from the pool only for the duration of
        the write, then returns it immediately.
        Workers hold ZERO persistent connections — connections are
        only occupied for the ~50ms it takes to executemany + commit.
        At any moment only as many connections are open as there are
        concurrent flushes, not as many as there are workers.
        """
        if not updates:
            return

        def _write():
            db_conn = pool.getconn()
            try:
                with db_conn.cursor() as cur:
                    cur.executemany(UPDATE_SQL, updates)
                db_conn.commit()
            except Exception:
                db_conn.rollback()
                raise
            finally:
                pool.putconn(db_conn)

        try:
            await asyncio.to_thread(_write)
            await stats.increment(rows_classified=len(updates))
        except Exception as exc:
            log.error(
                "DB flush FAILED for %d rows — data may be lost: %s",
                len(updates), exc, exc_info=True
            )
            await stats.increment(flush_errors=1)

    # ── Progress logger ────────────────────────────────────────
    stop_progress = asyncio.Event()

    async def progress_logger():
        """
        Logs progress every PROGRESS_INTERVAL rows.
        Stops when stop_progress event is set by the worker cleanup.
        Runs as a background task — never blocks workers.
        """
        last_logged = 0
        t_start     = time.monotonic()

        while not stop_progress.is_set():
            try:
                await asyncio.wait_for(
                    asyncio.shield(stop_progress.wait()),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                pass  # just a tick — continue logging

            classified = stats.rows_classified
            if classified == 0:
                continue

            if classified - last_logged >= PROGRESS_INTERVAL:
                elapsed   = time.monotonic() - t_start
                rate      = classified / elapsed if elapsed > 0 else 0
                remaining = max(0, stats.unique_to_process - classified)
                eta       = remaining / rate if rate > 0 else 0
                pct       = 100 * classified / max(stats.unique_to_process, 1)

                cost_so_far = (
                    (stats.total_input_tokens  * INPUT_PRICE_PER_1M  / 1_000_000) +
                    (stats.total_output_tokens * OUTPUT_PRICE_PER_1M / 1_000_000)
                ) * (1 + PLATFORM_FEE)

                log.info(
                    "\n  ┌─ PROGRESS ──────────────────────────────────┐\n"
                    "  │ Classified  : %s / %s (%.1f%%)\n"
                    "  │ Rate        : %.0f rec/s\n"
                    "  │ ETA         : ~%s\n"
                    "  │ Queue size  : %d / %d\n"
                    "  │ Failed      : %s\n"
                    "  │ Parse errors: %s\n"
                    "  │ Flush errors: %s\n"
                    "  │ Input tokens: %s\n"
                    "  │ Output tokens: %s\n"
                    "  │ Cost so far : $%.4f\n"
                    "  └─────────────────────────────────────────────┘",
                    f"{classified:,}",
                    f"{stats.unique_to_process:,}",
                    pct,
                    rate,
                    str(timedelta(seconds=int(eta))),
                    queue.qsize(),
                    QUEUE_MAXSIZE,
                    f"{stats.rows_failed:,}",
                    f"{stats.parse_errors:,}",
                    f"{stats.flush_errors:,}",
                    f"{stats.total_input_tokens:,}",
                    f"{stats.total_output_tokens:,}",
                    cost_so_far
                )

                # Save checkpoint
                checkpoint["rows_classified"]     = classified
                checkpoint["rows_failed"]         = stats.rows_failed
                checkpoint["total_input_tokens"]  = stats.total_input_tokens
                checkpoint["total_output_tokens"] = stats.total_output_tokens
                save_checkpoint(checkpoint, args.checkpoint)

                last_logged = classified

        # Final progress log when stopped
        classified = stats.rows_classified
        elapsed    = time.monotonic() - t_start
        rate       = classified / elapsed if elapsed > 0 else 0
        log.info(
            "Progress logger stopped. Final: %s classified | %.0f rec/s avg",
            f"{classified:,}", rate
        )

    # ── Worker ────────────────────────────────────────────────
    async def worker(worker_id, client):
        """
        Each worker:
        1. Gets a record from the queue
        2. Calls OpenRouter (no DB connection held here)
        3. Accumulates results in local buffer
        4. Borrows a connection, flushes, returns it immediately
        5. Stops on poison pill (None)
        Workers hold NO persistent DB connections.
        A connection is borrowed only for the ~50ms flush window.
        """
        updates = []

        try:
            while True:
                item = await queue.get()

                if item is None:
                    # Poison pill — flush remaining and stop
                    if updates:
                        await flush_to_db(updates)
                    break

                row_id, reacting_id, description = item
                subject = ing_names.get(row_id, "Drug A")
                partner = ing_names.get(reacting_id, "Drug B")

                severity, mechanism = await call_openrouter(
                    client, subject, partner, description
                )

                updates.append((severity, mechanism, row_id, reacting_id))

                # Flush every WORKER_FLUSH_SIZE records
                if len(updates) >= WORKER_FLUSH_SIZE:
                    await flush_to_db(updates)
                    updates.clear()

        except Exception as exc:
            log.error("Worker %d fatal error: %s", worker_id, exc, exc_info=True)
            if updates:
                try:
                    await flush_to_db(updates)
                except Exception:
                    pass

    # ── Launch everything ──────────────────────────────────────
    client = AsyncOpenAI(
        api_key=args.openrouter_api_key,
        base_url=OPENROUTER_BASE_URL,
        default_headers=OPENROUTER_HEADERS,
        timeout=30.0
    )

    # Start progress logger as background task
    progress_task = asyncio.create_task(
        progress_logger(), name="progress"
    )

    # Start producer + all workers
    producer_and_workers = [
        asyncio.create_task(producer(), name="producer")
    ] + [
        asyncio.create_task(worker(i, client), name=f"worker-{i}")
        for i in range(args.workers)
    ]

    # Wait for producer and all workers to finish
    await asyncio.gather(*producer_and_workers, return_exceptions=True)

    # Signal progress logger to stop and wait for it
    stop_progress.set()
    await progress_task

    await client.close()

    # Save final checkpoint after all workers and progress logger done
    checkpoint["rows_classified"]     = stats.rows_classified
    checkpoint["rows_failed"]         = stats.rows_failed
    checkpoint["total_input_tokens"]  = stats.total_input_tokens
    checkpoint["total_output_tokens"] = stats.total_output_tokens
    save_checkpoint(checkpoint, args.checkpoint)
    log.info("Final checkpoint saved")

    log.info(
        "Stage 2 complete: %s classified | %s failed | %s parse errors",
        f"{stats.rows_classified:,}",
        f"{stats.rows_failed:,}",
        f"{stats.parse_errors:,}"
    )


# ──────────────────────────────────────────────────────────────
# Stage 3 — Mirror duplicates
# ──────────────────────────────────────────────────────────────

def stage3_mirror(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
    stats: Stats,
) -> None:
    log.info("=== Stage 3: Mirror A→B results to B→A rows ===")
    try:
        with conn.cursor() as cur:
            cur.execute(MIRROR_SQL)
            stats.rows_mirrored = cur.rowcount
        conn.commit()
        log.info("Stage 3 complete: %s rows mirrored", f"{stats.rows_mirrored:,}")
    except Exception as exc:
        conn.rollback()
        log.error("Stage 3 mirror failed: %s", exc, exc_info=True)


# ──────────────────────────────────────────────────────────────
# Stage 4 — Final verification
# ──────────────────────────────────────────────────────────────

def stage4_verify(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
) -> dict:
    log.info("=== Stage 4: Final verification ===")
    with conn.cursor() as cur:
        cur.execute(VERIFY_SQL)
        r = cur.fetchone()

    result = {
        "total_rows":      r[0], "still_unknown":   r[1],
        "contraindicated": r[2], "major":           r[3],
        "moderate":        r[4], "minor":           r[5],
        "has_mechanism":   r[6], "no_mechanism":    r[7],
    }
    classified = result["total_rows"] - result["still_unknown"]

    log.info(
        "Verification:\n"
        "  Total rows       : %s\n"
        "  Classified       : %s (%.1f%%)\n"
        "  Still unknown    : %s (%.1f%%)\n"
        "  contraindicated  : %s\n"
        "  major            : %s\n"
        "  moderate         : %s\n"
        "  minor            : %s\n"
        "  Has mechanism    : %s (%.1f%%)\n"
        "  No mechanism     : %s",
        f"{result['total_rows']:,}",
        f"{classified:,}",
        100 * classified / max(result["total_rows"], 1),
        f"{result['still_unknown']:,}",
        100 * result["still_unknown"] / max(result["total_rows"], 1),
        f"{result['contraindicated']:,}",
        f"{result['major']:,}",
        f"{result['moderate']:,}",
        f"{result['minor']:,}",
        f"{result['has_mechanism']:,}",
        100 * result["has_mechanism"] / max(result["total_rows"], 1),
        f"{result['no_mechanism']:,}",
    )
    return result


# ──────────────────────────────────────────────────────────────
# Final summary
# ──────────────────────────────────────────────────────────────

def print_final_summary(
    stats: Stats,
    verify: dict,
    args: argparse.Namespace,
    log: logging.Logger,
) -> None:
    elapsed     = time.monotonic() - stats.start_time
    elapsed_str = str(timedelta(seconds=int(elapsed)))
    classified  = verify["total_rows"] - verify["still_unknown"]
    prefiltered = (stats.prefilter_contraindicated +
                   stats.prefilter_major +
                   stats.prefilter_minor)

    # Real cost from actual token usage
    cost_input  = stats.total_input_tokens  * INPUT_PRICE_PER_1M  / 1_000_000
    cost_output = stats.total_output_tokens * OUTPUT_PRICE_PER_1M / 1_000_000
    cost_total  = (cost_input + cost_output) * (1 + PLATFORM_FEE)

    # Throughput
    elapsed_s = time.monotonic() - stats.start_time
    rate      = stats.rows_classified / elapsed_s if elapsed_s > 0 else 0

    msg = f"""
╬════════════════════════════════════════════════╗
  SEVERITY ENRICHMENT COMPLETE
╠════════════════════════════════════════════════╣
  Provider  : OpenRouter
  Model     : {MODEL}
  Workers   : {args.workers}

  Stage 1 — Pre-filter (regex SQL, free):
    contraindicated  : {stats.prefilter_contraindicated:>12,}
    major            : {stats.prefilter_major:>12,}
    minor            : {stats.prefilter_minor:>12,}
    Total            : {prefiltered:>12,}  ({100*prefiltered/TOTAL_ROWS:.1f}%)

  Stage 2 — LLM Classification:
    Sent to LLM      : {stats.unique_to_process:>12,}
    Classified       : {stats.rows_classified:>12,}
    Failed           : {stats.rows_failed:>12,}
    Parse errors     : {stats.parse_errors:>12,}
    Throughput       : {rate:>11.0f} rec/s

  Stage 3 — SQL Mirror:
    Rows mirrored    : {stats.rows_mirrored:>12,}

  Final State:
    Total rows       : {verify['total_rows']:>12,}
    Classified       : {classified:>12,}  ({100*classified/max(verify['total_rows'],1):.1f}%)
    Still unknown    : {verify['still_unknown']:>12,}
    Has mechanism    : {verify['has_mechanism']:>12,}

  Token Usage:
    Input tokens     : {stats.total_input_tokens:>12,}
    Output tokens    : {stats.total_output_tokens:>12,}

  Cost:
    Input            : ${cost_input:>10.4f}
    Output           : ${cost_output:>10.4f}
    Platform fee     : ${(cost_input+cost_output)*PLATFORM_FEE:>10.4f}
    TOTAL            : ${cost_total:>10.4f}

  Total time         : {elapsed_str}
  Checkpoint         : {args.checkpoint}
  Log file           : {args.log_file}
╚════════════════════════════════════════════════╝"""

    print(msg)
    log.info(msg)


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    if AsyncOpenAI is None:
        print(
            "ERROR: openai package not installed.\n"
            "Run: pip install openai>=1.0.0",
            file=sys.stderr
        )
        sys.exit(1)

    log  = setup_logging(args.log_file)

    log.info("Severity Enrichment Pipeline — OpenRouter + %s", MODEL)
    log.info("Workers: %d | Dry run: %s", args.workers, args.dry_run)

    # Auto-skip prefilter and mirror for limited test runs
    # to avoid accidentally modifying data outside the test scope
    if args.limit:
        log.info(
            "LIMIT mode: skipping pre-filter and mirror stages "
            "to keep test run isolated"
        )
        args.skip_prefilter = True
        args.skip_mirror    = True

    # Connect to DB
    conn = get_db_connection(args, log=log)

    # Check columns exist
    check_required_columns(conn, log)

    # Show table status
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), "
            "COUNT(*) FILTER (WHERE severity = 'unknown'), "
            "COUNT(*) FILTER (WHERE severity != 'unknown') "
            "FROM drugdb.ingredient_interactions"
        )
        total, unknown, done = cur.fetchone()

    log.info(
        "Table: %s total | %s classified | %s remaining",
        f"{total:,}", f"{done:,}", f"{unknown:,}"
    )

    if unknown == 0 and not args.dry_run:
        log.info("All rows already classified. Nothing to do.")
        conn.close()
        return

    # Load ingredient names into memory
    ing_names = load_ingredient_names(conn, log)

    # Create connection pool for workers
    pool = create_connection_pool(args, size=min(args.workers, 20))

    # Load or create checkpoint
    # Resume works automatically: FETCH_UNIQUE_SQL filters
    # WHERE severity = 'unknown' so already-classified rows
    # are skipped at the DB level — no extra tracking needed.
    if args.resume and os.path.exists(args.checkpoint):
        checkpoint = load_checkpoint(args.checkpoint)
        log.info("Resuming from checkpoint: %s", checkpoint)
    else:
        checkpoint = {
            "stage": "start",
            "rows_classified": 0,
            "rows_failed": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0
        }

    stats = Stats()

    # Restore stats from checkpoint if resuming
    if args.resume:
        stats.rows_classified     = checkpoint.get("rows_classified", 0)
        stats.rows_failed         = checkpoint.get("rows_failed", 0)
        stats.total_input_tokens  = checkpoint.get("total_input_tokens", 0)
        stats.total_output_tokens = checkpoint.get("total_output_tokens", 0)

    try:
        # Stage 1 — Pre-filter
        if not args.skip_prefilter:
            stage1_prefilter(conn, log, stats, dry_run=args.dry_run)

        # Stage 2 — Async classification
        asyncio.run(
            stage2_async_classify(
                args, conn, pool, ing_names,
                stats, log, checkpoint,
                dry_run=args.dry_run
            )
        )

        if args.dry_run:
            return

        # Stage 3 — Mirror
        if not args.skip_mirror:
            stage3_mirror(conn, log, stats)

        # Stage 4 — Verify
        verify = stage4_verify(conn, log)

        # Summary
        print_final_summary(stats, verify, args, log)

        checkpoint["stage"] = "complete"
        save_checkpoint(checkpoint, args.checkpoint)

    except KeyboardInterrupt:
        log.warning("Interrupted — checkpoint saved")
        save_checkpoint(checkpoint, args.checkpoint)
    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=True)
        save_checkpoint(checkpoint, args.checkpoint)
        sys.exit(1)
    finally:
        conn.close()
        pool.closeall()


if __name__ == "__main__":
    main()
