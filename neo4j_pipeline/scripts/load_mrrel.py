import psycopg2
import logging
import time
import os
import io
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

MRREL_PATH = "/home/nathanivikas890_gmail_com/umls/MRREL.RRF"

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     5432,
    "dbname":   "postgres",
    "user":     "postgres",
    "password": os.environ.get("DB_PASSWORD", ""),
    "connect_timeout": 30,
}

SCHEMA     = "umls"
TABLE      = f"{SCHEMA}.mrrel"
CHUNK_SIZE = 500_000  # commit every 500k rows

CREATE_SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    cui1     VARCHAR(8),
    aui1     VARCHAR(10),
    stype1   VARCHAR(50),
    rel      VARCHAR(4),
    cui2     VARCHAR(8),
    aui2     VARCHAR(10),
    stype2   VARCHAR(50),
    rela     VARCHAR(100),
    rui      VARCHAR(11),
    srui     VARCHAR(50),
    sab      VARCHAR(40),
    sl       VARCHAR(40),
    rg       VARCHAR(10),
    dir      VARCHAR(1),
    suppress VARCHAR(1),
    cvf      VARCHAR(50)
);
"""

INDEXES = [
    ("idx_mrrel_cui1", f"CREATE INDEX IF NOT EXISTS idx_mrrel_cui1 ON {TABLE}(cui1);"),
    ("idx_mrrel_cui2", f"CREATE INDEX IF NOT EXISTS idx_mrrel_cui2 ON {TABLE}(cui2);"),
    ("idx_mrrel_rel",  f"CREATE INDEX IF NOT EXISTS idx_mrrel_rel  ON {TABLE}(rel);"),
    ("idx_mrrel_rela", f"CREATE INDEX IF NOT EXISTS idx_mrrel_rela ON {TABLE}(rela);"),
    ("idx_mrrel_sab",  f"CREATE INDEX IF NOT EXISTS idx_mrrel_sab  ON {TABLE}(sab);"),
    ("idx_mrrel_rui",  f"CREATE INDEX IF NOT EXISTS idx_mrrel_rui  ON {TABLE}(rui);"),
]


def get_last_loaded(cur):
    """Get last successfully committed row count to support resume."""
    cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
    return cur.fetchone()[0]


def load_mrrel():
    logger.info(f"File size: {os.path.getsize(MRREL_PATH)/(1024**3):.2f} GB")

    logger.info("Connecting to PostgreSQL at 178.236.185.230...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SET synchronous_commit = ON;")  # ON so each commit is visible immediately
    cur.execute("SET work_mem = '256MB';")
    cur.execute("SET maintenance_work_mem = '512MB';")

    cur.execute(CREATE_SCHEMA_SQL)
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    # Resume support — skip already loaded rows
    already_loaded = get_last_loaded(cur)
    if already_loaded > 0:
        logger.info(f"Resuming from row {already_loaded:,} (skipping already loaded rows)...")
    else:
        logger.info("Starting fresh load...")

    logger.info(f"Committing every {CHUNK_SIZE:,} rows so data is visible progressively...")
    print()

    start      = time.time()
    chunk_buf  = []
    total_done = already_loaded
    skipped    = 0
    line_num   = 0

    with tqdm(unit=" rows", desc="  Loading",
              bar_format="{desc}: {n_fmt} rows [{elapsed}, {rate_fmt}]",
              colour="green", initial=already_loaded) as pbar:

        with open(MRREL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line_num += 1

                # Skip already loaded rows (resume support)
                if line_num <= already_loaded:
                    continue

                cols = line.rstrip("\n").split("|")[:16]
                if len(cols) != 16:
                    skipped += 1
                    continue

                row = tuple(c if c != "" else None for c in cols)
                chunk_buf.append(row)

                if len(chunk_buf) >= CHUNK_SIZE:
                    # Use COPY for this chunk via StringIO
                    chunk_io = io.StringIO()
                    for r in chunk_buf:
                        chunk_io.write("|".join("" if v is None else v for v in r) + "\n")
                    chunk_io.seek(0)

                    cur.copy_expert(f"""
                        COPY {TABLE} (cui1,aui1,stype1,rel,cui2,aui2,stype2,
                                      rela,rui,srui,sab,sl,rg,dir,suppress,cvf)
                        FROM STDIN WITH (FORMAT csv, DELIMITER '|', NULL '')
                    """, chunk_io)
                    conn.commit()

                    total_done += len(chunk_buf)
                    pbar.update(len(chunk_buf))
                    chunk_buf = []

        # Final chunk
        if chunk_buf:
            chunk_io = io.StringIO()
            for r in chunk_buf:
                chunk_io.write("|".join("" if v is None else v for v in r) + "\n")
            chunk_io.seek(0)

            cur.copy_expert(f"""
                COPY {TABLE} (cui1,aui1,stype1,rel,cui2,aui2,stype2,
                              rela,rui,srui,sab,sl,rg,dir,suppress,cvf)
                FROM STDIN WITH (FORMAT csv, DELIMITER '|', NULL '')
            """, chunk_io)
            conn.commit()
            total_done += len(chunk_buf)
            pbar.update(len(chunk_buf))

    print()
    elapsed = time.time() - start
    logger.info(f"✅ Loaded {total_done:,} rows in {elapsed:.1f}s ({elapsed/60:.1f} mins)")
    if skipped:
        logger.warning(f"   Skipped {skipped:,} malformed rows")

    # Indexes
    logger.info(f"Creating {len(INDEXES)} indexes...")
    print()
    for idx_name, sql in tqdm(INDEXES, desc="  Indexes", colour="yellow",
                               bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]"):
        tqdm.write(f"  → {idx_name}...")
        cur.execute(sql)
        conn.commit()

    print()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
    final_count = cur.fetchone()[0]
    logger.info(f"✅ Done! {final_count:,} rows in {TABLE}")
    logger.info(f"   Total time: {(time.time()-start)/60:.1f} mins")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_mrrel()