import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from neo4j import GraphDatabase

# ── Logging setup ─────────────────────────────────────────────────────────────
_run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
_log_dir = Path(__file__).resolve().parent.parent / "logs"
LOG_FILE = str(_log_dir / f"populate_drug_nodes_{_run_ts}.log")
FAILED_IDS_FILE = str(_log_dir / f"failed_formulation_ids_{_run_ts}.txt")

_fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("populate_drug_nodes")
logger.setLevel(logging.DEBUG)

_fh = logging.FileHandler(LOG_FILE)
_fh.setLevel(logging.INFO)
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
_ch.setFormatter(_fmt)
logger.addHandler(_ch)

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST     = os.environ.get("DB_HOST", "localhost")
PG_DBNAME   = "postgres"
PG_USER     = "postgres"
PG_PASSWORD = os.environ.get("DB_PASSWORD", "")
PG_SCHEMA   = "drugdb"
PG_TABLE    = "drug"

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")

BATCH_SIZE      = 500
PROGRESS_EVERY  = 10_000

CYPHER = """
UNWIND $rows AS row
MERGE (d:Drug {formulation_id: row.formulation_id})
SET d.generic_name        = row.generic_name,
    d.generic_formulation = row.generic_formulation,
    d.dosage_forms        = row.dosage_forms,
    d.pharmacologic_class = row.pharmacologic_class,
    d.therapeutic_class   = row.therapeutic_class,
    d.mechanism_class     = row.mechanism_class
"""


def _arr(val):
    """Return list for array columns; empty list for NULL."""
    return val if val is not None else []


def verify_state(pg_conn, neo4j_driver):
    """Pre-run verification — returns (pg_count, neo4j_count, constraint_ok)."""
    logger.info("=" * 60)
    logger.info("PRE-RUN VERIFICATION")
    logger.info("=" * 60)

    # PostgreSQL checks
    cur = pg_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{PG_TABLE}")
    pg_count = cur.fetchone()[0]
    logger.info(f"PostgreSQL {PG_SCHEMA}.{PG_TABLE} row count: {pg_count:,}")

    required_cols = {
        "formulation_id", "generic_name", "generic_formulation",
        "dosage_forms", "pharmacologic_class", "therapeutic_class", "mechanism_class",
    }
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema='{PG_SCHEMA}' AND table_name='{PG_TABLE}'"
    )
    existing_cols = {row[0] for row in cur.fetchall()}
    missing = required_cols - existing_cols
    if missing:
        logger.error(f"MISSING columns in Postgres table: {missing}")
    else:
        logger.info(f"All 7 required columns present: {sorted(required_cols)}")
    cur.close()
    pg_conn.commit()  # close transaction so named cursor can open cleanly

    # Neo4j checks
    with neo4j_driver.session() as s:
        constraints = list(s.run("SHOW CONSTRAINTS"))
        has_constraint = any(
            "formulation_id" in str(dict(c)) and "Drug" in str(dict(c))
            for c in constraints
        )
        if has_constraint:
            logger.info("Neo4j Drug.formulation_id UNIQUENESS constraint: EXISTS")
        else:
            logger.warning("Neo4j Drug.formulation_id constraint NOT FOUND — creating it now")
            s.run(
                "CREATE CONSTRAINT drug_formulation_id IF NOT EXISTS "
                "FOR (d:Drug) REQUIRE d.formulation_id IS UNIQUE"
            )
            logger.info("Constraint created successfully")

        neo4j_count = s.run("MATCH (d:Drug) RETURN count(d) AS cnt").single()["cnt"]
        logger.info(f"Neo4j Drug node count (baseline): {neo4j_count:,}")

    logger.info("=" * 60)
    return pg_count, neo4j_count, not missing


def run_population(pg_conn, neo4j_driver):
    """Main population loop. Returns (total_processed, failed_batches list)."""
    start_time = time.time()
    total_processed = 0
    batch_num = 0
    failed_batches = []  # list of lists of formulation_ids

    cur = pg_conn.cursor("drug_cursor")  # named server-side cursor
    cur.execute(
        f"SELECT formulation_id::text, generic_name, generic_formulation, "
        f"dosage_forms, pharmacologic_class, therapeutic_class, mechanism_class "
        f"FROM {PG_SCHEMA}.{PG_TABLE}"
    )

    logger.info("Starting data population ...")
    logger.info(f"Batch size: {BATCH_SIZE} | Progress log every: {PROGRESS_EVERY:,} rows")

    while True:
        raw_rows = cur.fetchmany(BATCH_SIZE)
        if not raw_rows:
            break

        batch_num += 1
        row_start = total_processed + 1
        row_end   = total_processed + len(raw_rows)

        batch = [
            {
                "formulation_id":    r[0],
                "generic_name":      r[1],
                "generic_formulation": r[2],
                "dosage_forms":      r[3],
                "pharmacologic_class": _arr(r[4]),
                "therapeutic_class":   _arr(r[5]),
                "mechanism_class":     _arr(r[6]),
            }
            for r in raw_rows
        ]

        try:
            with neo4j_driver.session() as s:
                s.run(CYPHER, rows=batch)
            total_processed += len(raw_rows)
            logger.info(
                f"Batch {batch_num} complete — rows {row_start:,} to {row_end:,} inserted successfully"
            )
        except Exception as exc:
            ids_in_batch = [r[0] for r in raw_rows]
            logger.error(
                f"Batch {batch_num} FAILED (rows {row_start:,}–{row_end:,}): {exc}"
            )
            logger.error(
                f"FAILED_BATCH {batch_num} formulation_ids: {ids_in_batch}"
            )
            failed_batches.append(ids_in_batch)
            total_processed += len(raw_rows)  # still advance counter

        # Progress report every PROGRESS_EVERY rows
        if total_processed % PROGRESS_EVERY < BATCH_SIZE or not raw_rows:
            elapsed = time.time() - start_time
            rps = total_processed / elapsed if elapsed > 0 else 0
            remaining_rows = max(0, 88983 - total_processed)
            eta_sec = remaining_rows / rps if rps > 0 else 0
            logger.info(
                f"Progress: {total_processed:,} rows processed | "
                f"Elapsed: {elapsed:.1f}s | "
                f"Rate: {rps:.0f} rows/s | "
                f"ETA: {eta_sec:.0f}s"
            )

    cur.close()
    elapsed = time.time() - start_time
    rps = total_processed / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("POPULATION COMPLETE")
    logger.info(f"Total rows processed : {total_processed:,}")
    logger.info(f"Failed batches       : {len(failed_batches)}")
    logger.info(f"Time taken           : {elapsed:.2f}s")
    logger.info(f"Rows per second      : {rps:.0f}")
    logger.info("=" * 60)

    return total_processed, failed_batches


def post_run_verification(pg_conn, neo4j_driver, log_file):
    """Post-run checks. Returns True if all pass."""
    logger.info("=" * 60)
    logger.info("POST-RUN VERIFICATION")
    logger.info("=" * 60)

    all_pass = True

    # 1. Postgres count
    cur = pg_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{PG_TABLE}")
    pg_count = cur.fetchone()[0]
    cur.close()
    pg_conn.commit()
    logger.info(f"[1] PostgreSQL Drug row count : {pg_count:,}")

    # 2. Neo4j node count
    with neo4j_driver.session() as s:
        neo4j_count = s.run("MATCH (d:Drug) RETURN count(d) AS cnt").single()["cnt"]
        logger.info(f"[2] Neo4j Drug node count     : {neo4j_count:,}")

        # 3. Count match
        if pg_count == neo4j_count:
            logger.info(f"[3] Count match               : PASS ({pg_count:,} == {neo4j_count:,})")
        else:
            logger.error(f"[3] Count match               : FAIL (PG={pg_count:,} vs Neo4j={neo4j_count:,})")
            all_pass = False

        # 4. Sample 5 nodes
        samples = list(s.run(
            "MATCH (d:Drug) RETURN d LIMIT 5"
        ))
        logger.info(f"[4] Sample Drug nodes (5):")
        array_populated = False
        for i, rec in enumerate(samples, 1):
            node = dict(rec["d"])
            logger.info(f"    Node {i}: {node}")
            if (
                isinstance(node.get("pharmacologic_class"), list) or
                isinstance(node.get("therapeutic_class"), list) or
                isinstance(node.get("mechanism_class"), list)
            ):
                array_populated = True

        # 5. Array columns check
        if array_populated:
            logger.info("[5] Array columns (pharmacologic/therapeutic/mechanism_class): PASS")
        else:
            logger.error("[5] Array columns                                           : FAIL — no list properties found in sample")
            all_pass = False

    # 6. Log file path
    logger.info(f"[6] Log file location: {log_file}")

    logger.info("=" * 60)
    logger.info("FINAL SUMMARY")
    logger.info(f"  Total Neo4j Drug nodes : {neo4j_count:,}")
    logger.info(f"  Count match            : {'PASS' if pg_count == neo4j_count else 'FAIL'}")
    logger.info(f"  Array properties       : {'PASS' if array_populated else 'FAIL'}")
    logger.info(f"  Overall result         : {'PASS' if all_pass else 'FAIL'}")
    logger.info(f"  Log file               : {log_file}")
    logger.info("=" * 60)

    return all_pass


def main():
    start_wall = time.time()
    logger.info("=" * 60)
    logger.info("populate_drug_nodes.py — START")
    logger.info(f"Run timestamp   : {_run_ts}")
    logger.info(f"PostgreSQL host : {PG_HOST}  db={PG_DBNAME}  user={PG_USER}  table={PG_SCHEMA}.{PG_TABLE}")
    logger.info(f"Neo4j URI       : {NEO4J_URI}  user={NEO4J_USER}")
    logger.info(f"Log file        : {LOG_FILE}")
    logger.info("=" * 60)

    # Connect
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST, dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD
        )
        # autocommit must be OFF for named server-side cursors
        pg_conn.autocommit = False
        logger.info("PostgreSQL connection: OK")
    except Exception as e:
        logger.error(f"PostgreSQL connection FAILED: {e}")
        sys.exit(1)

    try:
        neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        neo4j_driver.verify_connectivity()
        logger.info("Neo4j connection: OK")
    except Exception as e:
        logger.error(f"Neo4j connection FAILED: {e}")
        pg_conn.close()
        sys.exit(1)

    # Pre-run verification
    verify_state(pg_conn, neo4j_driver)

    # Population
    total_processed, failed_batches = run_population(pg_conn, neo4j_driver)

    # Write failed IDs file
    if failed_batches:
        all_failed_ids = [fid for batch in failed_batches for fid in batch]
        with open(FAILED_IDS_FILE, "w") as f:
            f.write("\n".join(all_failed_ids))
        logger.info(
            f"{len(failed_batches)} batches failed — "
            f"{len(all_failed_ids)} formulation_ids written to {FAILED_IDS_FILE}"
        )
        print(f"\n*** {len(failed_batches)} batches failed — see log file for formulation_ids to reprocess ***")
        print(f"*** Failed IDs file: {FAILED_IDS_FILE} ***\n")

    # Post-run verification
    post_run_verification(pg_conn, neo4j_driver, LOG_FILE)

    pg_conn.close()
    neo4j_driver.close()

    total_elapsed = time.time() - start_wall
    logger.info(f"populate_drug_nodes.py — END  (total wall time: {total_elapsed:.2f}s)")


if __name__ == "__main__":
    main()
