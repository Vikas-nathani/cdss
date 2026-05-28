#!/usr/bin/env python3
"""
recover_from_openrouter_logs.py

Recovers ALL lost severity+mechanism results from OpenRouter
generation logs and writes them back to the DB.

Recovery pipeline:
1. Read generation IDs from CSV file (exported from openrouter.ai/logs)
2. Fetch prompt + completion for each ID via
   GET /api/v1/generation/content?id={gen_id}
3. Parse drug names from prompt user message
4. Parse severity + mechanism from completion JSON
5. Match to DB rows via drug name lookup
6. Write recovered results to DB

How to run:
  # Dry run first
  python3 scripts/recover_from_openrouter_logs.py \\
      --openrouter-api-key YOUR_KEY \\
      --db-password YOUR_PASSWORD \\
      --csv-file data/openrouter_generations.csv \\
      --dry-run

  # Full recovery
  python3 scripts/recover_from_openrouter_logs.py \\
      --openrouter-api-key YOUR_KEY \\
      --db-password YOUR_PASSWORD \\
      --csv-file data/openrouter_generations.csv
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time

import psycopg2
import requests

try:
    import aiohttp
except ImportError:
    aiohttp = None


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────

SEVERITY_VALUES = frozenset({
    "contraindicated", "major", "moderate", "minor", "unknown"
})


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Recover severity+mechanism from OpenRouter generation logs"
    )
    p.add_argument(
        "--csv-file",
        default="data/openrouter_generations.csv",
        help="Path to OpenRouter CSV export file"
    )
    p.add_argument("--openrouter-api-key", required=True)
    p.add_argument("--db-password",        required=True)
    p.add_argument("--db-host", default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--db-port", type=int, default=5432)
    p.add_argument("--db-name", default="postgres")
    p.add_argument("--db-user", default="postgres")
    p.add_argument("--dry-run",  action="store_true",
                   help="Show what would be recovered — no DB writes")
    p.add_argument("--log-file", default="logs/recover_openrouter.log")
    return p.parse_args()


# ──────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────

def setup_logging(log_file: str) -> logging.Logger:
    log = logging.getLogger("recover_openrouter")
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
    log: logging.Logger,
) -> psycopg2.extensions.connection:
    for attempt in range(1, 4):
        try:
            return psycopg2.connect(
                host=args.db_host, port=args.db_port,
                dbname=args.db_name, user=args.db_user,
                password=args.db_password, connect_timeout=30,
            )
        except psycopg2.OperationalError as exc:
            log.error("DB connection failed (attempt %d/3): %s", attempt, exc)
            if attempt >= 3:
                raise
            time.sleep(10)


# ──────────────────────────────────────────────────────────────
# Step 1 — Load generation IDs from CSV
# ──────────────────────────────────────────────────────────────

def load_generation_ids_from_csv(csv_path: str, log: logging.Logger) -> list:
    """
    Load generation IDs from OpenRouter exported CSV.
    Filters to CDSS app only. Skips cancelled and non-stop rows.
    Tries tab-delimited first, falls back to comma-delimited.
    """
    gen_ids = []
    skipped = 0

    with open(csv_path, newline='', encoding='utf-8') as f:
        # Sniff delimiter — OpenRouter exports are tab-separated
        sample = f.read(4096)
        f.seek(0)
        delimiter = '\t' if '\t' in sample else ','
        log.info("CSV delimiter detected: %r", delimiter)

        reader = csv.DictReader(f, delimiter=delimiter)
        headers = reader.fieldnames or []
        log.info("CSV columns: %s", headers)

        for row in reader:
            gen_id    = row.get("generation_id", "").strip()
            app_name  = row.get("app_name", "").strip()
            cancelled = row.get("cancelled", "").strip().upper()
            finish    = row.get("finish_reason_normalized", "").strip()

            # Only process CDSS app generations
            if "CDSS" not in app_name and app_name:
                skipped += 1
                continue

            # Skip cancelled
            if cancelled == "TRUE":
                skipped += 1
                continue

            # Skip non-stop finish reasons (error, length, etc.)
            if finish and finish != "stop":
                skipped += 1
                continue

            if gen_id:
                gen_ids.append(gen_id)

    log.info(
        "Loaded %s generation IDs from CSV (%s skipped)",
        f"{len(gen_ids):,}", f"{skipped:,}"
    )
    return gen_ids


# ──────────────────────────────────────────────────────────────
# Step 2 — Fetch prompt + completion for all IDs (async, cached)
# ──────────────────────────────────────────────────────────────

async def fetch_all_content(
    gen_ids: list,
    api_key: str,
    log: logging.Logger,
) -> dict:
    """
    Fetch prompt + completion for all generation IDs.
    50 concurrent requests.
    Saves results to a local JSONL cache file so we never
    need to re-fetch if the script is interrupted.
    """
    cache_file = "data/openrouter_content_cache.jsonl"
    os.makedirs("data", exist_ok=True)

    # Load already-fetched results from cache
    cached = {}
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            for line in f:
                try:
                    item = json.loads(line)
                    cached[item["id"]] = item["data"]
                except Exception:
                    pass
        log.info(
            "Loaded %s cached results from %s",
            f"{len(cached):,}", cache_file
        )

    # Only fetch what's not cached
    to_fetch = [gid for gid in gen_ids if gid not in cached]
    log.info(
        "Need to fetch %s / %s generations (rest from cache)",
        f"{len(to_fetch):,}", f"{len(gen_ids):,}"
    )

    if not to_fetch:
        return cached

    semaphore = asyncio.Semaphore(50)
    results   = dict(cached)

    cache_fh = open(cache_file, "a", buffering=1)

    async def fetch_one(session, gen_id):
        async with semaphore:
            for attempt in range(3):
                try:
                    async with session.get(
                        "https://openrouter.ai/api/v1/generation/content",
                        params={"id": gen_id},
                        timeout=aiohttp.ClientTimeout(total=20)
                    ) as resp:
                        if resp.status == 200:
                            data    = await resp.json()
                            content = data.get("data", {})
                            if content:
                                cache_fh.write(
                                    json.dumps({"id": gen_id, "data": content}) + "\n"
                                )
                                return gen_id, content
                            return gen_id, None
                        elif resp.status == 429:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        elif resp.status == 404:
                            return gen_id, None
                        else:
                            log.warning(
                                "Gen %s: status %d", gen_id[:20], resp.status
                            )
                            return gen_id, None
                except asyncio.TimeoutError:
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        log.warning("Timeout fetching gen %s", gen_id[:20])
                except Exception as exc:
                    if attempt < 2:
                        await asyncio.sleep(1)
                    else:
                        log.warning("Failed gen %s: %s", gen_id[:20], exc)
            return gen_id, None

    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            total      = len(to_fetch)
            done       = 0
            batch_size = 500

            for i in range(0, total, batch_size):
                batch         = to_fetch[i:i + batch_size]
                tasks         = [fetch_one(session, gid) for gid in batch]
                batch_results = await asyncio.gather(*tasks)

                for gen_id, data in batch_results:
                    if data:
                        results[gen_id] = data

                done += len(batch)
                log.info(
                    "Fetched: %s / %s (%.1f%%) | Successful: %s",
                    f"{done:,}", f"{total:,}",
                    100 * done / total,
                    f"{len(results):,}"
                )

                await asyncio.sleep(0.05)
    finally:
        cache_fh.close()

    log.info(
        "Content fetch complete: %s successful out of %s",
        f"{len(results):,}", f"{len(gen_ids):,}"
    )
    return results


# ──────────────────────────────────────────────────────────────
# Step 3 — Parse prompt user message
# ──────────────────────────────────────────────────────────────

def parse_user_message(messages: list):
    for msg in messages:
        if msg.get("role") == "user":
            content = msg.get("content", "")
            lines   = content.strip().split('\n', 1)
            if not lines:
                return None, None, None

            first_line = lines[0]

            if not first_line.startswith("A:"):
                return None, None, None

            # Remove "A:" prefix then split on " B:" to handle drug names with spaces
            rest  = first_line[2:]
            b_idx = rest.find(" B:")
            if b_idx == -1:
                return None, None, None

            subject_name = rest[:b_idx].strip()
            partner_name = rest[b_idx + 3:].strip()
            description  = lines[1].strip() if len(lines) > 1 else ""

            return subject_name, partner_name, description

    return None, None, None


# ──────────────────────────────────────────────────────────────
# Step 4 — Parse completion JSON
# ──────────────────────────────────────────────────────────────

def parse_completion(completion_text: str):
    if not completion_text:
        return "unknown", None
    try:
        parsed    = json.loads(completion_text.strip())
        severity  = parsed.get("severity", "unknown")
        mechanism = parsed.get("mechanism") or None
        if severity not in SEVERITY_VALUES:
            severity = "unknown"
        return severity, mechanism
    except Exception:
        return "unknown", None


# ──────────────────────────────────────────────────────────────
# Step 5 — Build DB lookup map (unknown rows only)
# ──────────────────────────────────────────────────────────────

def build_description_to_ids_map(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
) -> dict:
    log.info("Building description lookup map from DB...")
    desc_map = {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                ii.id::text,
                ii.reacting_id::text,
                ii.description,
                LOWER(subj.name) AS subject_name,
                LOWER(part.name) AS partner_name
            FROM drugdb.ingredient_interactions ii
            JOIN drugdb.ingredients subj ON subj.id = ii.id
            JOIN drugdb.ingredients part ON part.id = ii.reacting_id
            WHERE ii.severity = 'unknown'
        """)
        for row_id, reacting_id, description, subj_name, part_name in cur:
            key = (subj_name, part_name)
            if key not in desc_map:
                desc_map[key] = []
            desc_map[key].append((row_id, reacting_id))

    log.info("Loaded %s rows into lookup map", f"{len(desc_map):,}")
    return desc_map


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    if aiohttp is None:
        print(
            "ERROR: aiohttp not installed.\n"
            "Run: pip install aiohttp --break-system-packages",
            file=sys.stderr
        )
        sys.exit(1)

    log = setup_logging(args.log_file)
    log.info("OpenRouter Generation Recovery")
    log.info("Dry run: %s", args.dry_run)

    # Check CSV file exists
    if not os.path.exists(args.csv_file):
        log.error(
            "CSV file not found: %s\n"
            "Upload the OpenRouter CSV export to this path.",
            args.csv_file
        )
        sys.exit(1)

    # Connect to DB
    conn = get_db_connection(args, log)
    log.info("DB connected: %s:%d/%s", args.db_host, args.db_port, args.db_name)

    # Build DB lookup map (only rows still unknown)
    desc_map = build_description_to_ids_map(conn, log)
    log.info("Lookup map has %s unique drug pairs", f"{len(desc_map):,}")

    # Load generation IDs from CSV
    gen_ids = load_generation_ids_from_csv(args.csv_file, log)
    if not gen_ids:
        log.error("No generation IDs found in CSV")
        sys.exit(1)

    # Fetch all prompt + completion content (with caching)
    log.info("Fetching content for %s generations...", f"{len(gen_ids):,}")
    all_content = asyncio.run(
        fetch_all_content(gen_ids, args.openrouter_api_key, log)
    )

    # Parse and match each generation to DB rows
    updates         = []
    matched         = 0
    not_matched     = 0
    parse_errors    = 0
    skipped_unknown = 0

    for gen_id, detail in all_content.items():
        if not detail:
            continue

        input_data  = detail.get("input", {})
        output_data = detail.get("output", {})
        messages    = input_data.get("messages", [])
        completion  = output_data.get("completion", "") or ""

        subject_name, partner_name, description = parse_user_message(messages)
        if not subject_name or not partner_name:
            parse_errors += 1
            continue

        severity, mechanism = parse_completion(completion)

        if severity == "unknown":
            skipped_unknown += 1
            continue

        key  = (subject_name.lower(), partner_name.lower())
        rows = desc_map.get(key, [])

        if not rows:
            not_matched += 1
            continue

        for row_id, reacting_id in rows:
            updates.append((severity, mechanism, row_id, reacting_id))
            matched += 1

    log.info(
        "\n  Recovery parsing complete:\n"
        "  Total generations fetched  : %s\n"
        "  Matched to DB rows         : %s\n"
        "  Not matched                : %s\n"
        "  Parse errors               : %s\n"
        "  Skipped (severity=unknown) : %s\n"
        "  Updates to write           : %s",
        f"{len(all_content):,}",
        f"{matched:,}",
        f"{not_matched:,}",
        f"{parse_errors:,}",
        f"{skipped_unknown:,}",
        f"{len(updates):,}"
    )

    if args.dry_run:
        log.info("DRY RUN — no DB writes. Sample of first 10:")
        for severity, mechanism, row_id, reacting_id in updates[:10]:
            log.info(
                "  %s | severity=%-15s | mechanism=%s",
                row_id[:8], severity, mechanism
            )
        return

    if not updates:
        log.info("Nothing to write.")
        return

    # Write to DB in batches of 1000
    UPDATE_SQL = """
        UPDATE drugdb.ingredient_interactions
        SET severity = %s, mechanism = %s
        WHERE id = %s::uuid
          AND reacting_id = %s::uuid
          AND severity = 'unknown'
    """

    written = 0
    with conn.cursor() as cur:
        for i in range(0, len(updates), 1000):
            chunk = updates[i:i + 1000]
            cur.executemany(UPDATE_SQL, chunk)
            conn.commit()
            written += len(chunk)
            log.info(
                "Written %s / %s rows to DB",
                f"{written:,}", f"{len(updates):,}"
            )

    log.info("Recovery complete: %s rows recovered", f"{written:,}")

    # Verification
    with conn.cursor() as cur:
        cur.execute(
            "SELECT "
            "COUNT(*) FILTER (WHERE severity != 'unknown') AS classified, "
            "COUNT(*) FILTER (WHERE severity = 'unknown')  AS remaining "
            "FROM drugdb.ingredient_interactions"
        )
        classified, remaining = cur.fetchone()

    log.info(
        "DB state after recovery:\n"
        "  Classified : %s\n"
        "  Remaining  : %s",
        f"{classified:,}", f"{remaining:,}"
    )

    conn.close()


if __name__ == "__main__":
    main()
