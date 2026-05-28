-- Chain detail report — 5 samples per category (all_pass / partial_pass / none_pass)
-- Columns: drug_id_1mg, ib_rxcui_array, salt_composition,
--          ing_rxcui, ing_unii, dml_rxcui_array, dml_unii_array, master_linkage_id
-- One row per (brand, matched_rxcui). none_pass shows NULLs for chain columns.

WITH unique_rxcuis AS (
  SELECT DISTINCT unnest(rxcui) AS rxcui
  FROM drugdb.indian_brand
  WHERE match_combination = 'drugbank' AND rxcui IS NOT NULL
),
passing_rxcuis AS (
  SELECT DISTINCT u.rxcui
  FROM unique_rxcuis u
  JOIN drugdb.ingredients i ON i.rxcui = u.rxcui
  JOIN public."DrugMasterLinkage" dml ON dml.unii_ids @> ARRAY[i.unii::text]
  WHERE i.unii IS NOT NULL AND array_length(dml.rxcui_ids, 1) = 1
),
record_counts AS (
  SELECT
    ib.indian_brand_id,
    array_length(ib.rxcui, 1)  AS total_rxcuis,
    COUNT(p.rxcui)             AS passing_rxcuis
  FROM drugdb.indian_brand ib
  CROSS JOIN LATERAL unnest(ib.rxcui) AS r(rxcui)
  LEFT JOIN passing_rxcuis p ON p.rxcui = r.rxcui
  WHERE ib.match_combination = 'drugbank' AND ib.rxcui IS NOT NULL
  GROUP BY ib.indian_brand_id, ib.rxcui
),
sampled_ids AS (
  (SELECT indian_brand_id, 'all_pass'     AS category FROM record_counts WHERE passing_rxcuis = total_rxcuis AND passing_rxcuis > 0 LIMIT 5)
  UNION ALL
  (SELECT indian_brand_id, 'partial_pass' AS category FROM record_counts WHERE passing_rxcuis >= 1 AND passing_rxcuis < total_rxcuis LIMIT 5)
  UNION ALL
  (SELECT indian_brand_id, 'none_pass'    AS category FROM record_counts WHERE passing_rxcuis = 0 LIMIT 5)
)
-- all_pass + partial_pass: show each passing rxcui with full chain detail (one row per rxcui)
(
  SELECT DISTINCT ON (s.category, ib.drug_id_1mg, r.rxcui)
    s.category,
    ib.drug_id_1mg,
    ib.rxcui                  AS ib_rxcui_array,
    ib.salt_composition,
    i.rxcui                   AS ing_rxcui,
    i.unii                    AS ing_unii,
    dml.rxcui_ids             AS dml_rxcui_array,
    dml.unii_ids              AS dml_unii_array,
    dml.master_linkage_id
  FROM sampled_ids s
  JOIN drugdb.indian_brand ib ON ib.indian_brand_id = s.indian_brand_id
  CROSS JOIN LATERAL unnest(ib.rxcui) AS r(rxcui)
  JOIN passing_rxcuis pr ON pr.rxcui = r.rxcui
  JOIN drugdb.ingredients i ON i.rxcui = r.rxcui
  JOIN public."DrugMasterLinkage" dml ON dml.unii_ids @> ARRAY[i.unii::text]
  WHERE s.category IN ('all_pass', 'partial_pass')
    AND i.unii IS NOT NULL
    AND array_length(dml.rxcui_ids, 1) = 1
  ORDER BY s.category, ib.drug_id_1mg, r.rxcui
)
UNION ALL
-- none_pass: no chain match, chain columns are NULL
(
  SELECT
    s.category,
    ib.drug_id_1mg,
    ib.rxcui                  AS ib_rxcui_array,
    ib.salt_composition,
    NULL                      AS ing_rxcui,
    NULL                      AS ing_unii,
    NULL                      AS dml_rxcui_array,
    NULL                      AS dml_unii_array,
    NULL                      AS master_linkage_id
  FROM sampled_ids s
  JOIN drugdb.indian_brand ib ON ib.indian_brand_id = s.indian_brand_id
  WHERE s.category = 'none_pass'
)
ORDER BY category, drug_id_1mg;
