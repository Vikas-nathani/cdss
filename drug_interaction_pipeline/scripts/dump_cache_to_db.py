#!/usr/bin/env python3
"""
dump_cache_to_db.py

Reads openrouter_content_cache.jsonl and writes
severity + mechanism to drugdb.ingredient_interactions
for rows where severity = 'unknown'.
No API calls. Pure local processing.
"""

import argparse
import json
import logging
import os
import sys
import time
import psycopg2

SEVERITY_VALUES = frozenset({
    "contraindicated", "major", "moderate", "minor", "unknown"
})

UPDATE_SQL = """
    UPDATE drugdb.ingredient_interactions
    SET severity = %s, mechanism = %s
    WHERE id = %s::uuid
      AND reacting_id = %s::uuid
      AND severity = 'unknown'
"""

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-password", required=True)
    p.add_argument("--db-host",     default=os.environ.get("DB_HOST", "localhost"))
    p.add_argument("--db-port",     type=int, default=5432)
    p.add_argument("--db-name",     default="postgres")
    p.add_argument("--db-user",     default="postgres")
    p.add_argument("--cache-file",  default="data/openrouter_content_cache.jsonl")
    p.add_argument("--log-file",    default="logs/dump_cache_to_db.log")
    return p.parse_args()

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
    log = logging.getLogger("dump_cache")
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(ch)
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    return log

def parse_user_message(messages):
    for msg in messages:
        if msg.get("role") == "user":
            content = msg.get("content", "")
            lines   = content.strip().split('\n', 1)
            if not lines:
                return None, None
            first_line = lines[0]
            if not first_line.startswith("A:"):
                return None, None
            rest  = first_line[2:]
            b_idx = rest.find(" B:")
            if b_idx == -1:
                return None, None
            subject_name = rest[:b_idx].strip()
            partner_name = rest[b_idx + 3:].strip()
            return subject_name, partner_name
    return None, None

def parse_completion(completion_text):
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

def main():
    args = parse_args()
    log  = setup_logging(args.log_file)

    log.info("Dump cache to DB — starting")

    conn = psycopg2.connect(
        host=args.db_host, port=args.db_port,
        dbname=args.db_name, user=args.db_user,
        password=args.db_password, connect_timeout=30
    )
    log.info("DB connected")

    # Build lookup map (subject_name, partner_name) -> [(id, reacting_id)]
    log.info("Building name lookup map from DB...")
    t0       = time.monotonic()
    name_map = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                ii.id::text,
                ii.reacting_id::text,
                LOWER(subj.name) AS subject_name,
                LOWER(part.name) AS partner_name
            FROM drugdb.ingredient_interactions ii
            JOIN drugdb.ingredients subj ON subj.id = ii.id
            JOIN drugdb.ingredients part ON part.id = ii.reacting_id
            WHERE ii.severity = 'unknown'
        """)
        for row_id, reacting_id, subj, part in cur:
            key = (subj, part)
            if key not in name_map:
                name_map[key] = []
            name_map[key].append((row_id, reacting_id))
    log.info(
        "Loaded %s unique pairs in %.1fs",
        f"{len(name_map):,}", time.monotonic() - t0
    )

    # Read cache and build updates
    log.info("Reading cache file: %s", args.cache_file)
    updates      = []
    matched      = 0
    not_matched  = 0
    parse_errors = 0
    skipped      = 0
    total_lines  = 0

    with open(args.cache_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total_lines += 1

            try:
                item        = json.loads(line)
                detail      = item.get("data", {})
                input_data  = detail.get("input", {})
                output_data = detail.get("output", {})
                messages    = input_data.get("messages", [])
                completion  = output_data.get("completion", "") or ""

                subject_name, partner_name = parse_user_message(messages)
                if not subject_name or not partner_name:
                    parse_errors += 1
                    continue

                severity, mechanism = parse_completion(completion)

                if severity == "unknown":
                    skipped += 1
                    continue

                key  = (subject_name.lower(), partner_name.lower())
                rows = name_map.get(key, [])

                if not rows:
                    not_matched += 1
                    continue

                for row_id, reacting_id in rows:
                    updates.append((severity, mechanism, row_id, reacting_id))
                    matched += 1

            except Exception:
                parse_errors += 1

            if total_lines % 50_000 == 0:
                log.info(
                    "Processed %s lines | matched=%s | not_matched=%s",
                    f"{total_lines:,}", f"{matched:,}", f"{not_matched:,}"
                )

    log.info(
        "\n  Cache processing complete:\n"
        "  Total lines:      %s\n"
        "  Matched:          %s\n"
        "  Not matched:      %s\n"
        "  Parse errors:     %s\n"
        "  Skipped unknown:  %s\n"
        "  Updates to write: %s",
        f"{total_lines:,}",
        f"{matched:,}",
        f"{not_matched:,}",
        f"{parse_errors:,}",
        f"{skipped:,}",
        f"{len(updates):,}"
    )

    if not updates:
        log.info("Nothing to write — exiting")
        conn.close()
        return

    # Write to DB in batches of 1000
    log.info("Writing %s rows to DB...", f"{len(updates):,}")
    written = 0
    with conn.cursor() as cur:
        for i in range(0, len(updates), 1000):
            chunk = updates[i:i + 1000]
            cur.executemany(UPDATE_SQL, chunk)
            conn.commit()
            written += len(chunk)
            if written % 50_000 == 0:
                log.info(
                    "Written %s / %s rows",
                    f"{written:,}", f"{len(updates):,}"
                )

    log.info("Done! Written %s rows to DB", f"{written:,}")

    # Final count
    with conn.cursor() as cur:
        cur.execute(
            "SELECT "
            "COUNT(*) FILTER (WHERE severity != 'unknown') AS classified,"
            "COUNT(*) FILTER (WHERE severity = 'unknown')  AS remaining "
            "FROM drugdb.ingredient_interactions"
        )
        classified, remaining = cur.fetchone()

    log.info(
        "\n  DB final state:\n"
        "  Classified : %s\n"
        "  Remaining  : %s",
        f"{classified:,}", f"{remaining:,}"
    )

    conn.close()

if __name__ == "__main__":
    main()
