"""
Optimized version: computes passing rxcuis from 939 unique values first,
then checks 149k indian_brand records against that result set.
GIN scan on DrugMasterLinkage runs 939 times instead of 313k times.
"""

import os
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

QUERY = """
WITH unique_rxcuis AS (
  -- Step 1: only 939 unique rxcuis — compute this set once
  SELECT DISTINCT unnest(rxcui) AS rxcui
  FROM drugdb.indian_brand
  WHERE match_combination = 'drugbank'
    AND rxcui IS NOT NULL
),
passing_rxcuis AS (
  -- Step 2: of those 939, find which pass the full chain
  -- DISTINCT ensures one row per rxcui — prevents double-counting in record_counts
  SELECT DISTINCT u.rxcui
  FROM unique_rxcuis u
  JOIN drugdb.ingredients i ON i.rxcui = u.rxcui
  JOIN public."DrugMasterLinkage" dml ON dml.unii_ids @> ARRAY[i.unii::text]
  WHERE i.unii IS NOT NULL
    AND array_length(dml.rxcui_ids, 1) = 1
),
record_counts AS (
  -- Step 3: unnest rxcui array first so planner can use hash join
  SELECT
    ib.indian_brand_id,
    array_length(ib.rxcui, 1)                                     AS total_rxcuis,
    COUNT(p.rxcui)                                                 AS passing_rxcuis
  FROM drugdb.indian_brand ib
  CROSS JOIN LATERAL unnest(ib.rxcui) AS r(rxcui)
  LEFT JOIN passing_rxcuis p ON p.rxcui = r.rxcui
  WHERE ib.match_combination = 'drugbank'
    AND ib.rxcui IS NOT NULL
  GROUP BY ib.indian_brand_id, ib.rxcui
)
SELECT
  COUNT(*)                                                                       AS total_records,
  COUNT(*) FILTER (WHERE passing_rxcuis >= 1)                                   AS at_least_one_passes,
  COUNT(*) FILTER (WHERE passing_rxcuis = total_rxcuis AND passing_rxcuis > 0)  AS all_rxcuis_pass
FROM record_counts;
"""

def main():
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:Blumax%24dev@178.236.185.230:5432/postgres")
    db_url = db_url.replace("%24", "$")

    print("Connecting to database...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("Running optimized chain query...")
    t0 = time.time()
    cur.execute(QUERY)
    row = cur.fetchone()
    elapsed = time.time() - t0

    total, at_least_one, all_pass = row
    print(f"\nResults (query took {elapsed:.1f}s):")
    print(f"  Total indian_brand records (drugbank, rxcui not null) : {total:,}")
    print(f"  At least ONE rxcui passes full chain                  : {at_least_one:,}")
    print(f"  ALL rxcuis pass full chain                            : {all_pass:,}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
