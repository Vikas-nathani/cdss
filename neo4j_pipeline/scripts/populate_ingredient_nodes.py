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
LOG_FILE = str(_log_dir / f"populate_ingredient_nodes_{_run_ts}.log")
FAILED_IDS_FILE = str(_log_dir / f"failed_ingredient_ids_{_run_ts}.txt")

_fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("populate_ingredient_nodes")
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
PROGRESS_EVERY = 5_000

CYPHER_INGREDIENTS = """
UNWIND $rows AS row
MERGE (i:Ingredient {ingredient_id: row.ingredient_id})
SET i.name        = row.name,
    i.rxcui       = row.rxcui,
    i.drugbank_id = row.drugbank_id,
    i.unii        = row.unii
"""

CYPHER_RELATIONSHIPS = """
UNWIND $rows AS row
MATCH (d:Drug {formulation_id: row.formulation_id})
MATCH (i:Ingredient {ingredient_id: row.ingredient_id})
MERGE (d)-[r:CONTAINS_ACTIVE]->(i)
SET r.strength = row.strength,
    r.mass     = row.mass,
    r.unit     = row.unit
"""


def _str(val):
    return val if val is not None else ""


def _opt(val):
    return val if val is not None else None


def verify_pre_run(pg_conn, neo4j_driver):
    logger.info("=" * 60)
    logger.info("PRE-RUN VERIFICATION")
    logger.info("=" * 60)

    cur = pg_conn.cursor()

    cur.execute("SELECT COUNT(*) FROM drugdb.ingredients")
    ing_pg = cur.fetchone()[0]
    logger.info(f"PostgreSQL drugdb.ingredients row count     : {ing_pg:,}")

    cur.execute("SELECT COUNT(*) FROM drugdb.drug_ingredient_mapping")
    map_pg = cur.fetchone()[0]
    logger.info(f"PostgreSQL drugdb.drug_ingredient_mapping   : {map_pg:,}")

    cur.close()
    pg_conn.commit()

    with neo4j_driver.session() as s:
        drug_cnt = s.run("MATCH (d:Drug) RETURN count(d) AS c").single()["c"]
        logger.info(f"Neo4j Drug nodes                           : {drug_cnt:,}")
        if drug_cnt != 88983:
            logger.warning(f"Expected 88,983 Drug nodes — found {drug_cnt:,}")

        # Check / create Ingredient constraint
        constraints = list(s.run("SHOW CONSTRAINTS"))
        has_ing_constraint = any(
            "ingredient_id" in str(dict(c)) and "Ingredient" in str(dict(c))
            for c in constraints
        )
        if has_ing_constraint:
            logger.info("Neo4j Ingredient.ingredient_id constraint  : EXISTS")
        else:
            logger.warning("Ingredient.ingredient_id constraint NOT FOUND — creating it")
            s.run(
                "CREATE CONSTRAINT ingredient_id_unique IF NOT EXISTS "
                "FOR (i:Ingredient) REQUIRE i.ingredient_id IS UNIQUE"
            )
            logger.info("Constraint ingredient_id_unique created successfully")

        ing_baseline = s.run("MATCH (i:Ingredient) RETURN count(i) AS c").single()["c"]
        logger.info(f"Neo4j Ingredient nodes (baseline)          : {ing_baseline:,}")

        rel_baseline = s.run(
            "MATCH ()-[r:CONTAINS_ACTIVE]->() RETURN count(r) AS c"
        ).single()["c"]
        logger.info(f"Neo4j CONTAINS_ACTIVE relationships (baseline): {rel_baseline:,}")

    logger.info("=" * 60)
    return ing_pg, map_pg


# ── STEP 1: Ingredient nodes ───────────────────────────────────────────────────

def populate_ingredients(pg_conn, neo4j_driver):
    logger.info("=" * 60)
    logger.info("STEP 1 — Creating Ingredient nodes")
    logger.info("=" * 60)
    start = time.time()
    total = 0
    batch_num = 0
    failed_batches = []

    cur = pg_conn.cursor("ingredient_cursor")
    cur.execute(
        "SELECT id::text, name, rxcui, drugbank_id, unii "
        "FROM drugdb.ingredients"
    )

    while True:
        raw = cur.fetchmany(BATCH_SIZE)
        if not raw:
            break

        batch_num += 1
        row_start = total + 1
        row_end   = total + len(raw)

        batch = [
            {
                "ingredient_id": r[0],
                "name":          _str(r[1]),
                "rxcui":         _str(r[2]),
                "drugbank_id":   _opt(r[3]),
                "unii":          _opt(r[4]),
            }
            for r in raw
        ]

        try:
            with neo4j_driver.session() as s:
                s.run(CYPHER_INGREDIENTS, rows=batch)
            total += len(raw)
            logger.info(
                f"Batch {batch_num} complete — rows {row_start:,} to {row_end:,} inserted"
            )
        except Exception as exc:
            ids = [r[0] for r in raw]
            logger.error(f"Batch {batch_num} FAILED (rows {row_start:,}–{row_end:,}): {exc}")
            logger.error(f"FAILED_BATCH {batch_num} ingredient_ids: {ids}")
            failed_batches.append(ids)
            total += len(raw)

        if total % PROGRESS_EVERY < BATCH_SIZE:
            elapsed = time.time() - start
            rps = total / elapsed if elapsed > 0 else 0
            logger.info(
                f"Progress: {total:,} ingredients | Elapsed: {elapsed:.1f}s | "
                f"Rate: {rps:.0f} rows/s"
            )

    cur.close()
    pg_conn.commit()

    elapsed = time.time() - start
    rps = total / elapsed if elapsed > 0 else 0
    logger.info("─" * 60)
    logger.info(f"STEP 1 COMPLETE — {total:,} rows processed | "
                f"{len(failed_batches)} failed batches | {rps:.0f} rows/s")
    logger.info("─" * 60)
    return total, failed_batches


# ── STEP 2: CONTAINS_ACTIVE relationships ─────────────────────────────────────

def populate_relationships(pg_conn, neo4j_driver):
    logger.info("=" * 60)
    logger.info("STEP 2 — Creating CONTAINS_ACTIVE relationships")
    logger.info("=" * 60)
    start = time.time()
    total = 0
    batch_num = 0
    failed_batches = []

    cur = pg_conn.cursor("mapping_cursor")
    cur.execute(
        "SELECT formulation_id::text, ingredient_id::text, "
        "mass::float, unit "
        "FROM drugdb.drug_ingredient_mapping"
    )

    while True:
        raw = cur.fetchmany(BATCH_SIZE)
        if not raw:
            break

        batch_num += 1
        row_start = total + 1
        row_end   = total + len(raw)

        batch = []
        for r in raw:
            mass = r[2]
            unit = r[3] or ""
            strength = f"{mass} {unit}".strip() if mass is not None else ""
            batch.append({
                "formulation_id": r[0],
                "ingredient_id":  r[1],
                "mass":           mass,
                "unit":           unit,
                "strength":       strength,
            })

        try:
            with neo4j_driver.session() as s:
                result = s.run(CYPHER_RELATIONSHIPS, rows=batch)
                summary = result.consume()
                created = summary.counters.relationships_created
                matched = len(batch) - created  # already existed (re-run)
                total += len(raw)
                logger.info(
                    f"Batch {batch_num} complete — rows {row_start:,} to {row_end:,} | "
                    f"created: {created}, already existed: {matched}"
                )
        except Exception as exc:
            ids = [(r[0], r[1]) for r in raw]
            logger.error(f"Batch {batch_num} FAILED (rows {row_start:,}–{row_end:,}): {exc}")
            logger.error(f"FAILED_BATCH {batch_num} (formulation_id, ingredient_id): {ids}")
            failed_batches.append([(r[0], r[1]) for r in raw])
            total += len(raw)

        if total % PROGRESS_EVERY < BATCH_SIZE:
            elapsed = time.time() - start
            rps = total / elapsed if elapsed > 0 else 0
            logger.info(
                f"Progress: {total:,} mappings | Elapsed: {elapsed:.1f}s | "
                f"Rate: {rps:.0f} rows/s"
            )

    cur.close()
    pg_conn.commit()

    elapsed = time.time() - start
    rps = total / elapsed if elapsed > 0 else 0
    logger.info("─" * 60)
    logger.info(f"STEP 2 COMPLETE — {total:,} rows processed | "
                f"{len(failed_batches)} failed batches | {rps:.0f} rows/s")
    logger.info("─" * 60)
    return total, failed_batches


# ── Post-run verification ──────────────────────────────────────────────────────

def verify_post_run(pg_conn, neo4j_driver):
    logger.info("=" * 60)
    logger.info("POST-RUN VERIFICATION")
    logger.info("=" * 60)
    all_pass = True

    cur = pg_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM drugdb.ingredients")
    pg_ing = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM drugdb.drug_ingredient_mapping")
    pg_map = cur.fetchone()[0]
    cur.close()
    pg_conn.commit()

    with neo4j_driver.session() as s:
        neo4j_ing = s.run("MATCH (i:Ingredient) RETURN count(i) AS c").single()["c"]
        neo4j_rel = s.run(
            "MATCH ()-[r:CONTAINS_ACTIVE]->() RETURN count(r) AS c"
        ).single()["c"]

        # 1. Ingredient count match
        if pg_ing == neo4j_ing:
            logger.info(f"[1] Ingredient count match: PASS ({pg_ing:,} == {neo4j_ing:,})")
        else:
            logger.error(f"[1] Ingredient count match: FAIL (PG={pg_ing:,} vs Neo4j={neo4j_ing:,})")
            all_pass = False

        # 2. Relationship count match
        if pg_map == neo4j_rel:
            logger.info(f"[2] CONTAINS_ACTIVE count match: PASS ({pg_map:,} == {neo4j_rel:,})")
        else:
            logger.warning(
                f"[2] CONTAINS_ACTIVE count match: "
                f"PG={pg_map:,} vs Neo4j={neo4j_rel:,} "
                f"(diff={pg_map - neo4j_rel:,} — may be due to missing Drug/Ingredient nodes)"
            )

        # 3. Sample 5 Ingredient nodes
        samples = list(s.run("MATCH (i:Ingredient) RETURN i LIMIT 5"))
        logger.info("[3] Sample Ingredient nodes (5):")
        has_drugbank = False
        for idx, rec in enumerate(samples, 1):
            node = dict(rec["i"])
            logger.info(f"    Node {idx}: {node}")
            if node.get("drugbank_id"):
                has_drugbank = True

        # 4. Sample 5 CONTAINS_ACTIVE relationships
        rels = list(s.run(
            "MATCH (d:Drug)-[r:CONTAINS_ACTIVE]->(i:Ingredient) "
            "RETURN d.generic_name AS drug, i.name AS ingredient, "
            "r.strength AS strength, r.mass AS mass, r.unit AS unit LIMIT 5"
        ))
        logger.info("[4] Sample CONTAINS_ACTIVE relationships (5):")
        for idx, rec in enumerate(rels, 1):
            logger.info(
                f"    {idx}. {rec['drug']} --[strength={rec['strength']}]--> {rec['ingredient']}"
            )

        # 5. drugbank_id populated
        if has_drugbank:
            logger.info("[5] drugbank_id populated on at least one node: PASS")
        else:
            logger.error("[5] drugbank_id populated on at least one node: FAIL")
            all_pass = False

    # 6. Log file
    logger.info(f"[6] Log file: {LOG_FILE}")

    logger.info("=" * 60)
    logger.info("FINAL SUMMARY")
    logger.info(f"  Total Ingredient nodes     : {neo4j_ing:,}")
    logger.info(f"  Total CONTAINS_ACTIVE rels : {neo4j_rel:,}")
    logger.info(f"  Ingredient count match     : {'PASS' if pg_ing == neo4j_ing else 'FAIL'}")
    logger.info(f"  Relationship count match   : {'PASS' if pg_map == neo4j_rel else 'SEE LOG'}")
    logger.info(f"  drugbank_id populated      : {'PASS' if has_drugbank else 'FAIL'}")
    logger.info(f"  Overall                    : {'PASS' if all_pass else 'FAIL'}")
    logger.info(f"  Log file                   : {LOG_FILE}")
    logger.info("=" * 60)
    return all_pass


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    wall_start = time.time()
    logger.info("=" * 60)
    logger.info("populate_ingredient_nodes.py — START")
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

    verify_pre_run(pg_conn, neo4j_driver)

    ing_total, ing_failed = populate_ingredients(pg_conn, neo4j_driver)
    rel_total, rel_failed = populate_relationships(pg_conn, neo4j_driver)

    all_failed_ids = []
    all_failed_ids += [fid for batch in ing_failed for fid in batch]
    all_failed_ids += [f"{fid[0]}|{fid[1]}" for batch in rel_failed for fid in batch]

    if all_failed_ids:
        with open(FAILED_IDS_FILE, "w") as f:
            f.write("\n".join(str(x) for x in all_failed_ids))
        logger.info(
            f"{len(ing_failed)} ingredient batch(es) + {len(rel_failed)} mapping batch(es) failed — "
            f"IDs written to {FAILED_IDS_FILE}"
        )
        print(f"\n*** Failed IDs file: {FAILED_IDS_FILE} ***\n")

    verify_post_run(pg_conn, neo4j_driver)

    pg_conn.close()
    neo4j_driver.close()

    wall_elapsed = time.time() - wall_start
    logger.info(f"populate_ingredient_nodes.py — END  (total wall time: {wall_elapsed:.2f}s)")


if __name__ == "__main__":
    main()
