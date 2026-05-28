"""
populate_drug_interactions.py

Creates INTERACTS_WITH relationships between Ingredient nodes in Neo4j.

DATA MODEL NOTE:
  The source table drugdb.ingredient_interactions stores interactions at the
  INGREDIENT level (id → ingredient UUID, reacting_id → ingredient UUID).
  These are NOT drug formulation_ids. Mapping to drug-drug pairs via
  drug_ingredient_mapping would produce hundreds of millions of relationships
  (avg 58 drugs per interacting ingredient × 2.9M pairs), making it
  impractical and semantically redundant.

  The correct graph model for a CDSS is:
    Drug -[:CONTAINS_ACTIVE]-> Ingredient -[:INTERACTS_WITH]-> Ingredient
                                                    <-[:CONTAINS_ACTIVE]- Drug

  Drug-drug interaction checking then uses a 3-hop graph pattern:
    MATCH (a:Drug)-[:CONTAINS_ACTIVE]->(i:Ingredient)
          -[:INTERACTS_WITH]->(j:Ingredient)
          <-[:CONTAINS_ACTIVE]-(b:Drug)
    RETURN a, b, i, j

  Columns used from ingredient_interactions:
    id          → subject Ingredient.ingredient_id
    reacting_id → partner Ingredient.ingredient_id
    severity    → r.severity
    mechanism   → r.mechanism
    description → r.description
"""

import logging
import os
import sys
import time
from datetime import datetime

import psycopg2
from neo4j import GraphDatabase

# ── Logging setup ─────────────────────────────────────────────────────────────
_run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = (
    f"/home/nathanivikas890_gmail_com/cdss/populate_drug_interactions_{_run_ts}.log"
)
FAILED_FILE = (
    f"/home/nathanivikas890_gmail_com/cdss/failed_interactions_{_run_ts}.txt"
)

_fmt = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("populate_drug_interactions")
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

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")

BATCH_SIZE     = 1000
PROGRESS_EVERY = 50_000
TOTAL_EXPECTED = 2_910_556

CYPHER = """
UNWIND $rows AS row
MATCH (a:Ingredient {ingredient_id: row.subject_ingredient_id})
MATCH (b:Ingredient {ingredient_id: row.partner_ingredient_id})
MERGE (a)-[r:INTERACTS_WITH]->(b)
SET r.severity    = row.severity,
    r.mechanism   = row.mechanism,
    r.description = row.description
"""


def verify_pre_run(pg_conn, neo4j_driver):
    logger.info("=" * 65)
    logger.info("PRE-RUN VERIFICATION")
    logger.info("=" * 65)

    cur = pg_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM drugdb.ingredient_interactions")
    pg_count = cur.fetchone()[0]
    logger.info(f"PostgreSQL ingredient_interactions row count : {pg_count:,}")
    cur.close()
    pg_conn.commit()

    with neo4j_driver.session() as s:
        drug_cnt = s.run("MATCH (d:Drug) RETURN count(d) AS c").single()["c"]
        logger.info(f"Neo4j Drug nodes                            : {drug_cnt:,}")

        ing_cnt = s.run("MATCH (i:Ingredient) RETURN count(i) AS c").single()["c"]
        logger.info(f"Neo4j Ingredient nodes                      : {ing_cnt:,}")

        rel_cnt = s.run(
            "MATCH ()-[r:INTERACTS_WITH]->() RETURN count(r) AS c"
        ).single()["c"]
        logger.info(f"Neo4j INTERACTS_WITH rels (baseline)        : {rel_cnt:,}")

        # Check index on Ingredient.ingredient_id
        indexes = list(s.run("SHOW INDEXES"))
        ing_idx = any(
            "Ingredient" in str(dict(x)) and "ingredient_id" in str(dict(x))
            for x in indexes
        )
        if ing_idx:
            logger.info("Index on Ingredient.ingredient_id           : EXISTS")
        else:
            logger.warning("Index on Ingredient.ingredient_id NOT FOUND — creating it")
            s.run(
                "CREATE INDEX ingredient_id_idx IF NOT EXISTS "
                "FOR (i:Ingredient) ON (i.ingredient_id)"
            )
            logger.info("Index created")

    logger.info("")
    logger.info("DATA MODEL NOTE:")
    logger.info("  ingredient_interactions uses ingredient UUIDs (not formulation_ids).")
    logger.info("  Creating INTERACTS_WITH between Ingredient nodes — NOT Drug nodes.")
    logger.info("  Drug-drug interaction queries traverse the 3-hop path:")
    logger.info("    Drug -[:CONTAINS_ACTIVE]-> Ingredient")
    logger.info("         -[:INTERACTS_WITH]->  Ingredient")
    logger.info("         <-[:CONTAINS_ACTIVE]- Drug")
    logger.info("")
    # Estimated time at ~4000 rows/sec
    est_sec = TOTAL_EXPECTED / 4000
    logger.info(f"Estimated time @ 4,000 rows/s: {est_sec/60:.1f} min ({est_sec:.0f}s)")
    logger.info("=" * 65)
    return pg_count


def run_population(pg_conn, neo4j_driver):
    logger.info("=" * 65)
    logger.info("POPULATING INTERACTS_WITH relationships")
    logger.info("=" * 65)
    start = time.time()
    total_processed = 0
    total_created = 0
    batch_num = 0
    failed_batches = []
    zero_created_batches = 0

    cur = pg_conn.cursor("interactions_cursor")
    cur.execute(
        "SELECT id::text, reacting_id::text, severity, mechanism, description "
        "FROM drugdb.ingredient_interactions"
    )

    while True:
        raw = cur.fetchmany(BATCH_SIZE)
        if not raw:
            break

        batch_num += 1
        row_start = total_processed + 1
        row_end   = total_processed + len(raw)

        batch = [
            {
                "subject_ingredient_id": r[0],
                "partner_ingredient_id": r[1],
                "severity":              r[2],   # may be None
                "mechanism":             r[3],   # may be None
                "description":           r[4],   # may be None
            }
            for r in raw
        ]

        try:
            with neo4j_driver.session() as s:
                result = s.run(CYPHER, rows=batch)
                summary = result.consume()
                created = summary.counters.relationships_created
                total_created += created
                total_processed += len(raw)

                if created == 0:
                    zero_created_batches += 1
                    subject_ids = [r[0] for r in raw]
                    logger.warning(
                        f"Batch {batch_num} (rows {row_start:,}–{row_end:,}): "
                        f"0 relationships created — possible unmatched ingredients. "
                        f"Subject IDs sample: {subject_ids[:5]}"
                    )

        except Exception as exc:
            ids = [(r[0], r[1]) for r in raw]
            logger.error(
                f"Batch {batch_num} FAILED (rows {row_start:,}–{row_end:,}): {exc}"
            )
            logger.error(f"FAILED_BATCH {batch_num}: {ids[:10]} ...")
            failed_batches.append(ids)
            total_processed += len(raw)

        # Progress every PROGRESS_EVERY rows
        if total_processed % PROGRESS_EVERY < BATCH_SIZE:
            elapsed = time.time() - start
            rps = total_processed / elapsed if elapsed > 0 else 0
            remaining = max(0, TOTAL_EXPECTED - total_processed)
            eta = remaining / rps if rps > 0 else 0
            pct = total_processed / TOTAL_EXPECTED * 100
            logger.info(
                f"Progress: {total_processed:,} / {TOTAL_EXPECTED:,} ({pct:.1f}%) | "
                f"Created: {total_created:,} | "
                f"Elapsed: {elapsed:.0f}s | "
                f"Rate: {rps:.0f} rows/s | "
                f"ETA: {eta:.0f}s"
            )

    cur.close()
    pg_conn.commit()

    elapsed = time.time() - start
    rps = total_processed / elapsed if elapsed > 0 else 0

    logger.info("=" * 65)
    logger.info("POPULATION COMPLETE")
    logger.info(f"Total rows processed          : {total_processed:,}")
    logger.info(f"Total relationships created   : {total_created:,}")
    logger.info(f"Batches with 0 created        : {zero_created_batches}")
    logger.info(f"Failed batches                : {len(failed_batches)}")
    logger.info(f"Unmatched / already existed   : {total_processed - total_created:,}")
    logger.info(f"Time taken                    : {elapsed:.2f}s")
    logger.info(f"Rows per second               : {rps:.0f}")
    logger.info("=" * 65)
    return total_processed, total_created, failed_batches


def verify_post_run(pg_conn, neo4j_driver, pg_total):
    logger.info("=" * 65)
    logger.info("POST-RUN VERIFICATION")
    logger.info("=" * 65)
    all_pass = True

    with neo4j_driver.session() as s:
        neo4j_rel = s.run(
            "MATCH ()-[r:INTERACTS_WITH]->() RETURN count(r) AS c"
        ).single()["c"]

        # 1. Count comparison
        logger.info(f"[1] PostgreSQL ingredient_interactions rows : {pg_total:,}")
        logger.info(f"    Neo4j INTERACTS_WITH relationships      : {neo4j_rel:,}")
        if pg_total == neo4j_rel:
            logger.info("    Count match: PASS")
        else:
            diff = pg_total - neo4j_rel
            logger.warning(
                f"    Count match: {neo4j_rel:,} created vs {pg_total:,} source rows "
                f"(diff={diff:,}). This is expected if any ingredient IDs in "
                f"ingredient_interactions have no Ingredient node, or if "
                f"duplicate (subject, partner) pairs exist in the source."
            )

        # 2. Sample 5 relationships
        samples = list(s.run(
            "MATCH (a:Ingredient)-[r:INTERACTS_WITH]->(b:Ingredient) "
            "RETURN a.name AS subject, b.name AS partner, "
            "r.severity AS severity, r.mechanism AS mechanism, "
            "r.description AS description LIMIT 5"
        ))
        logger.info("[2] Sample INTERACTS_WITH relationships (5):")
        for idx, rec in enumerate(samples, 1):
            logger.info(
                f"    {idx}. [{rec['severity']}] {rec['subject']} "
                f"--[{rec['mechanism']}]--> {rec['partner']}"
            )
            logger.info(f"       Description: {str(rec['description'])[:120]}")

        # 3. Breakdown by severity
        sev_counts = list(s.run(
            "MATCH ()-[r:INTERACTS_WITH]->() "
            "RETURN r.severity AS severity, COUNT(r) AS count "
            "ORDER BY count DESC"
        ))
        logger.info("[3] Breakdown by severity:")
        for rec in sev_counts:
            logger.info(f"    {rec['severity']}: {rec['count']:,}")

        # 4. Confirm no self-loops (an ingredient interacting with itself)
        self_loops = s.run(
            "MATCH (a:Ingredient)-[r:INTERACTS_WITH]->(a) RETURN count(r) AS c"
        ).single()["c"]
        if self_loops == 0:
            logger.info("[4] Self-loop check: PASS (0 self-loop relationships)")
        else:
            logger.warning(f"[4] Self-loop check: {self_loops:,} self-loop relationships found")

    logger.info(f"[5] Log file : {LOG_FILE}")

    logger.info("=" * 65)
    logger.info("FINAL SUMMARY")
    logger.info(f"  Total INTERACTS_WITH relationships : {neo4j_rel:,}")
    logger.info(f"  Source rows                        : {pg_total:,}")
    logger.info(f"  Count match                        : {'PASS' if pg_total == neo4j_rel else 'SEE LOG'}")
    logger.info(f"  Self-loops                         : {'PASS' if self_loops == 0 else 'WARNING'}")
    logger.info(f"  Overall                            : {'PASS' if all_pass else 'FAIL'}")
    logger.info(f"  Log file                           : {LOG_FILE}")
    logger.info("")
    logger.info("To query drug-drug interactions via this graph:")
    logger.info("  MATCH (a:Drug)-[:CONTAINS_ACTIVE]->(i:Ingredient)")
    logger.info("        -[:INTERACTS_WITH]->(j:Ingredient)")
    logger.info("        <-[:CONTAINS_ACTIVE]-(b:Drug)")
    logger.info("  WHERE a.generic_name = 'METFORMIN'")
    logger.info("  RETURN a.generic_name, b.generic_name, i.name, j.name,")
    logger.info("         r.severity, r.mechanism LIMIT 20")
    logger.info("=" * 65)
    return all_pass


def main():
    wall_start = time.time()
    logger.info("=" * 65)
    logger.info("populate_drug_interactions.py — START")
    logger.info(f"Run timestamp : {_run_ts}")
    logger.info(f"PostgreSQL    : {PG_HOST}  db={PG_DBNAME}  user={PG_USER}")
    logger.info(f"Neo4j URI     : {NEO4J_URI}  user={NEO4J_USER}")
    logger.info(f"Log file      : {LOG_FILE}")
    logger.info(f"Expected rows : {TOTAL_EXPECTED:,}")
    logger.info("=" * 65)

    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST, dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD
        )
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

    pg_total = verify_pre_run(pg_conn, neo4j_driver)

    total_processed, total_created, failed_batches = run_population(
        pg_conn, neo4j_driver
    )

    # Write failed rows to file
    if failed_batches:
        all_failed = [
            f"{pair[0]}|{pair[1]}"
            for batch in failed_batches
            for pair in batch
        ]
        with open(FAILED_FILE, "w") as f:
            f.write("\n".join(all_failed))
        logger.info(
            f"{len(failed_batches)} batch(es) failed — "
            f"{len(all_failed)} row identifiers written to {FAILED_FILE}"
        )
        print(f"\n*** {len(failed_batches)} batches failed — see {FAILED_FILE} ***\n")

    verify_post_run(pg_conn, neo4j_driver, pg_total)

    pg_conn.close()
    neo4j_driver.close()

    wall_elapsed = time.time() - wall_start
    logger.info(
        f"populate_drug_interactions.py — END  "
        f"(total wall time: {wall_elapsed:.2f}s)"
    )


if __name__ == "__main__":
    main()
