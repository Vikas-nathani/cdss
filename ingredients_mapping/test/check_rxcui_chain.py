"""
Counts indian_brand records (match_combination='drugbank') where:
  - at_least_one: at least 1 rxcui passes the full chain
  - all_pass:     every rxcui in the record passes the full chain

Full chain per rxcui:
  indian_brand.rxcui (unnested)
    -> drugdb.ingredients (join on rxcui)          -> get unii
    -> DrugMasterLinkage (unii_ids @> [unii])      -> filter array_length(rxcui_ids,1) = 1
"""

import os
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

QUERY = """
WITH source AS (
  SELECT indian_brand_id, unnest(rxcui) AS rxcui
  FROM drugdb.indian_brand
  WHERE match_combination = 'drugbank'
    AND rxcui IS NOT NULL
),
rxcui_pass AS (
  SELECT DISTINCT s.indian_brand_id, s.rxcui
  FROM source s
  JOIN drugdb.ingredients i ON i.rxcui = s.rxcui
  JOIN public."DrugMasterLinkage" dml ON dml.unii_ids @> ARRAY[i.unii::text]
  WHERE i.unii IS NOT NULL
    AND array_length(dml.rxcui_ids, 1) = 1
),
record_counts AS (
  SELECT
    ib.indian_brand_id,
    array_length(ib.rxcui, 1)  AS total_rxcuis,
    COUNT(rp.rxcui)             AS passing_rxcuis
  FROM drugdb.indian_brand ib
  LEFT JOIN rxcui_pass rp ON rp.indian_brand_id = ib.indian_brand_id
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
    # psycopg2 needs %24 decoded back to $
    db_url = db_url.replace("%24", "$")

    print("Connecting to database...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("Running chain query (may take a few seconds)...")
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
