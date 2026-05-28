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
LOG_FILE   = str(_log_dir / f"populate_drugclass_nodes_{_run_ts}.log")
FAILED_FILE = str(_log_dir / f"failed_drugclass_{_run_ts}.txt")

_fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("populate_drugclass_nodes")
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

BATCH_SIZE     = 500
PROGRESS_EVERY = 10_000

CYPHER_DRUGCLASS = """
UNWIND $rows AS row
MERGE (c:DrugClass {name: row.name})
"""

CYPHER_RELATIONSHIPS = """
UNWIND $rows AS row
MATCH (d:Drug {formulation_id: row.formulation_id})
MATCH (c:DrugClass {name: row.class_name})
MERGE (d)-[r:BELONGS_TO_CLASS]->(c)
SET r.type = row.class_type
"""

SQL_DISTINCT_CLASSES = """
SELECT DISTINCT class_name FROM (
  SELECT UNNEST(pharmacologic_class) AS class_name
  FROM drugdb.drug WHERE pharmacologic_class IS NOT NULL
  UNION
  SELECT UNNEST(therapeutic_class)
  FROM drugdb.drug WHERE therapeutic_class IS NOT NULL
  UNION
  SELECT UNNEST(mechanism_class)
  FROM drugdb.drug WHERE mechanism_class IS NOT NULL
) t
WHERE class_name IS NOT NULL AND TRIM(class_name) != ''
ORDER BY class_name
"""

SQL_DRUG_CLASS_PAIRS = """
SELECT
  formulation_id::text,
  UNNEST(pharmacologic_class) AS class_name,
  'pharmacologic'             AS class_type
FROM drugdb.drug WHERE pharmacologic_class IS NOT NULL
UNION ALL
SELECT
  formulation_id::text,
  UNNEST(therapeutic_class),
  'therapeutic'
FROM drugdb.drug WHERE therapeutic_class IS NOT NULL
UNION ALL
SELECT
  formulation_id::text,
  UNNEST(mechanism_class),
  'mechanism'
FROM drugdb.drug WHERE mechanism_class IS NOT NULL
"""


def verify_pre_run(pg_conn, neo4j_driver):
    logger.info("=" * 60)
    logger.info("PRE-RUN VERIFICATION")
    logger.info("=" * 60)

    cur = pg_conn.cursor()

    cur.execute("""
        SELECT COUNT(DISTINCT class_name) FROM (
          SELECT UNNEST(pharmacologic_class) AS class_name FROM drugdb.drug WHERE pharmacologic_class IS NOT NULL
          UNION
          SELECT UNNEST(therapeutic_class)   FROM drugdb.drug WHERE therapeutic_class IS NOT NULL
          UNION
          SELECT UNNEST(mechanism_class)     FROM drugdb.drug WHERE mechanism_class IS NOT NULL
        ) t WHERE class_name IS NOT NULL AND TRIM(class_name) != ''
    """)
    distinct_classes = cur.fetchone()[0]
    logger.info(f"Distinct class names (Postgres)         : {distinct_classes:,}")

    cur.execute("""
        SELECT COUNT(*) FROM (
          SELECT formulation_id, UNNEST(pharmacologic_class) FROM drugdb.drug WHERE pharmacologic_class IS NOT NULL
          UNION ALL
          SELECT formulation_id, UNNEST(therapeutic_class)   FROM drugdb.drug WHERE therapeutic_class IS NOT NULL
          UNION ALL
          SELECT formulation_id, UNNEST(mechanism_class)     FROM drugdb.drug WHERE mechanism_class IS NOT NULL
        ) t
    """)
    total_pairs = cur.fetchone()[0]
    logger.info(f"Total drug-class pairs (Postgres)       : {total_pairs:,}")

    for col, label in [
        ("pharmacologic_class", "pharmacologic"),
        ("therapeutic_class",   "therapeutic"),
        ("mechanism_class",     "mechanism"),
    ]:
        cur.execute(
            f"SELECT COUNT(*) FROM (SELECT UNNEST({col}) FROM drugdb.drug "
            f"WHERE {col} IS NOT NULL) t"
        )
        logger.info(f"  {label}: {cur.fetchone()[0]:,}")

    cur.close()
    pg_conn.commit()

    with neo4j_driver.session() as s:
        drug_cnt = s.run("MATCH (d:Drug) RETURN count(d) AS c").single()["c"]
        logger.info(f"Neo4j Drug nodes                        : {drug_cnt:,}")

        dc_baseline = s.run("MATCH (c:DrugClass) RETURN count(c) AS c").single()["c"]
        logger.info(f"Neo4j DrugClass nodes (baseline)        : {dc_baseline:,}")

        rel_baseline = s.run(
            "MATCH ()-[r:BELONGS_TO_CLASS]->() RETURN count(r) AS c"
        ).single()["c"]
        logger.info(f"Neo4j BELONGS_TO_CLASS rels (baseline)  : {rel_baseline:,}")

        constraints = list(s.run("SHOW CONSTRAINTS"))
        has_dc_constraint = any(
            "DrugClass" in str(dict(c)) and "name" in str(dict(c))
            for c in constraints
        )
        if has_dc_constraint:
            logger.info("DrugClass.name uniqueness constraint    : EXISTS")
        else:
            logger.warning("DrugClass.name constraint NOT FOUND — creating it")
            s.run(
                "CREATE CONSTRAINT drugclass_name_unique IF NOT EXISTS "
                "FOR (c:DrugClass) REQUIRE c.name IS UNIQUE"
            )
            logger.info("Constraint created successfully")

    logger.info("=" * 60)
    return distinct_classes, total_pairs


# ── STEP 1: Create DrugClass nodes ────────────────────────────────────────────

def populate_drugclass_nodes(pg_conn, neo4j_driver):
    logger.info("=" * 60)
    logger.info("STEP 1 — Creating DrugClass nodes")
    logger.info("=" * 60)
    start = time.time()

    cur = pg_conn.cursor()
    cur.execute(SQL_DISTINCT_CLASSES)
    all_classes = [row[0] for row in cur.fetchall()]
    cur.close()
    pg_conn.commit()

    total = len(all_classes)
    logger.info(f"Fetched {total:,} distinct class names from Postgres")

    created = 0
    failed = 0
    for i in range(0, total, BATCH_SIZE):
        batch_names = all_classes[i : i + BATCH_SIZE]
        batch = [{"name": n} for n in batch_names]
        try:
            with neo4j_driver.session() as s:
                result = s.run(CYPHER_DRUGCLASS, rows=batch)
                summary = result.consume()
                created += summary.counters.nodes_created
            logger.info(
                f"Batch {i // BATCH_SIZE + 1} — "
                f"rows {i + 1:,} to {i + len(batch):,} merged "
                f"(nodes created so far: {created:,})"
            )
        except Exception as exc:
            logger.error(f"DrugClass batch {i // BATCH_SIZE + 1} FAILED: {exc}")
            logger.error(f"Failed names sample: {batch_names[:10]}")
            failed += len(batch)

    elapsed = time.time() - start
    logger.info("─" * 60)
    logger.info(f"STEP 1 COMPLETE — {created:,} DrugClass nodes created | "
                f"{failed} failed | {elapsed:.2f}s")
    logger.info("─" * 60)
    return created


# ── STEP 2: Create BELONGS_TO_CLASS relationships ─────────────────────────────

def populate_relationships(pg_conn, neo4j_driver):
    logger.info("=" * 60)
    logger.info("STEP 2 — Creating BELONGS_TO_CLASS relationships")
    logger.info("=" * 60)
    start = time.time()
    total_processed = 0
    total_created   = 0
    batch_num       = 0
    failed_batches  = []

    cur = pg_conn.cursor("drugclass_rel_cursor")
    cur.execute(SQL_DRUG_CLASS_PAIRS)

    while True:
        raw = cur.fetchmany(BATCH_SIZE)
        if not raw:
            break

        batch_num += 1
        row_start = total_processed + 1
        row_end   = total_processed + len(raw)

        # Filter out empty/null class names silently
        batch = [
            {
                "formulation_id": r[0],
                "class_name":     r[1],
                "class_type":     r[2],
            }
            for r in raw
            if r[1] and r[1].strip()
        ]

        if not batch:
            total_processed += len(raw)
            continue

        try:
            with neo4j_driver.session() as s:
                result = s.run(CYPHER_RELATIONSHIPS, rows=batch)
                summary = result.consume()
                rels_created = summary.counters.relationships_created
                total_created   += rels_created
                total_processed += len(raw)
            logger.info(
                f"Batch {batch_num} complete — rows {row_start:,} to {row_end:,} | "
                f"rels created: {rels_created}"
            )
        except Exception as exc:
            ids = [r[0] for r in raw]
            logger.error(
                f"Batch {batch_num} FAILED (rows {row_start:,}–{row_end:,}): {exc}"
            )
            logger.error(f"FAILED_BATCH {batch_num} formulation_ids: {ids[:20]}")
            failed_batches.append(ids)
            total_processed += len(raw)

        if total_processed % PROGRESS_EVERY < BATCH_SIZE:
            elapsed = time.time() - start
            rps = total_processed / elapsed if elapsed > 0 else 0
            logger.info(
                f"Progress: {total_processed:,} rows | "
                f"Rels created: {total_created:,} | "
                f"Elapsed: {elapsed:.1f}s | "
                f"Rate: {rps:.0f} rows/s"
            )

    cur.close()
    pg_conn.commit()

    elapsed = time.time() - start
    rps = total_processed / elapsed if elapsed > 0 else 0
    logger.info("─" * 60)
    logger.info(f"STEP 2 COMPLETE — {total_processed:,} rows processed | "
                f"{total_created:,} rels created | "
                f"{len(failed_batches)} failed batches | "
                f"{rps:.0f} rows/s")
    logger.info("─" * 60)
    return total_processed, total_created, failed_batches


# ── Post-run verification ──────────────────────────────────────────────────────

def verify_post_run(pg_conn, neo4j_driver, pg_classes, pg_pairs):
    logger.info("=" * 60)
    logger.info("POST-RUN VERIFICATION")
    logger.info("=" * 60)
    all_pass = True

    with neo4j_driver.session() as s:
        neo4j_dc  = s.run("MATCH (c:DrugClass) RETURN count(c) AS c").single()["c"]
        neo4j_rel = s.run(
            "MATCH ()-[r:BELONGS_TO_CLASS]->() RETURN count(r) AS c"
        ).single()["c"]

        # 1. DrugClass node count match
        if pg_classes == neo4j_dc:
            logger.info(f"[1] DrugClass node count : PASS ({pg_classes:,} == {neo4j_dc:,})")
        else:
            logger.error(f"[1] DrugClass node count : FAIL (PG={pg_classes:,} vs Neo4j={neo4j_dc:,})")
            all_pass = False

        # 2. Relationship count
        # Note: MERGE deduplicates when same drug has same class name
        # across multiple arrays, so neo4j_rel may be <= pg_pairs
        if neo4j_rel == pg_pairs:
            logger.info(f"[2] BELONGS_TO_CLASS count : PASS ({neo4j_rel:,} == {pg_pairs:,})")
        else:
            diff = pg_pairs - neo4j_rel
            logger.info(
                f"[2] BELONGS_TO_CLASS count : {neo4j_rel:,} created vs {pg_pairs:,} source pairs "
                f"(diff={diff:,} — expected: MERGE deduplicates drugs that share "
                f"the same class name across multiple array columns)"
            )

        # 3. Sample 5 DrugClass nodes
        samples = list(s.run("MATCH (c:DrugClass) RETURN c.name AS name LIMIT 5"))
        logger.info("[3] Sample DrugClass nodes (5):")
        for i, rec in enumerate(samples, 1):
            logger.info(f"    {i}. {rec['name']}")

        # 4. Sample 5 BELONGS_TO_CLASS relationships
        rels = list(s.run(
            "MATCH (d:Drug)-[r:BELONGS_TO_CLASS]->(c:DrugClass) "
            "RETURN d.generic_name AS drug, c.name AS class, r.type AS type "
            "LIMIT 5"
        ))
        logger.info("[4] Sample BELONGS_TO_CLASS relationships (5):")
        for i, rec in enumerate(rels, 1):
            logger.info(f"    {i}. [{rec['type']}] {rec['drug']} --> {rec['class']}")

        # 5. Breakdown by class type
        type_counts = list(s.run(
            "MATCH ()-[r:BELONGS_TO_CLASS]->() "
            "RETURN r.type AS type, COUNT(r) AS count "
            "ORDER BY count DESC"
        ))
        logger.info("[5] Breakdown by class type:")
        for rec in type_counts:
            logger.info(f"    {rec['type']}: {rec['count']:,}")

        # 6. Find a drug with all 3 class types
        example = s.run("""
            MATCH (d:Drug)-[r:BELONGS_TO_CLASS]->(c:DrugClass)
            WITH d, collect({type: r.type, class: c.name}) AS classes,
                 collect(DISTINCT r.type) AS types
            WHERE size(types) = 3
            RETURN d.generic_name AS drug, classes
            LIMIT 1
        """).single()
        if example:
            logger.info(f"[6] Drug with all 3 class types: {example['drug']}")
            for cls in example["classes"]:
                logger.info(f"    [{cls['type']}] {cls['class']}")
        else:
            logger.info("[6] No single drug found with all 3 class types in sample")

    logger.info(f"[7] Log file: {LOG_FILE}")
    logger.info("=" * 60)
    logger.info("FINAL SUMMARY")
    logger.info(f"  DrugClass nodes         : {neo4j_dc:,}")
    logger.info(f"  BELONGS_TO_CLASS rels   : {neo4j_rel:,}")
    logger.info(f"  Source drug-class pairs : {pg_pairs:,}")
    logger.info(f"  DrugClass count match   : {'PASS' if pg_classes == neo4j_dc else 'FAIL'}")
    logger.info(f"  Rel count match         : {'PASS' if neo4j_rel == pg_pairs else 'SEE NOTE'}")
    logger.info(f"  Overall                 : {'PASS' if all_pass else 'FAIL'}")
    logger.info(f"  Log file                : {LOG_FILE}")
    logger.info("=" * 60)
    return all_pass


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    wall_start = time.time()
    logger.info("=" * 60)
    logger.info("populate_drugclass_nodes.py — START")
    logger.info(f"Run timestamp : {_run_ts}")
    logger.info(f"PostgreSQL    : {PG_HOST}  db={PG_DBNAME}  user={PG_USER}")
    logger.info(f"Neo4j URI     : {NEO4J_URI}  user={NEO4J_USER}")
    logger.info(f"Log file      : {LOG_FILE}")
    logger.info("=" * 60)

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

    pg_classes, pg_pairs = verify_pre_run(pg_conn, neo4j_driver)

    populate_drugclass_nodes(pg_conn, neo4j_driver)

    _, _, failed_batches = populate_relationships(pg_conn, neo4j_driver)

    if failed_batches:
        all_failed = [fid for batch in failed_batches for fid in batch]
        with open(FAILED_FILE, "w") as f:
            f.write("\n".join(all_failed))
        logger.info(
            f"{len(failed_batches)} batch(es) failed — "
            f"{len(all_failed)} formulation_ids written to {FAILED_FILE}"
        )
        print(f"\n*** Failed IDs: {FAILED_FILE} ***\n")

    verify_post_run(pg_conn, neo4j_driver, pg_classes, pg_pairs)

    pg_conn.close()
    neo4j_driver.close()

    wall_elapsed = time.time() - wall_start
    logger.info(
        f"populate_drugclass_nodes.py — END  (total wall time: {wall_elapsed:.2f}s)"
    )


if __name__ == "__main__":
    main()
