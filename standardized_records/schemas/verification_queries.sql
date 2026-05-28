-- ============================================================================
-- PIPELINE VERIFICATION QUERIES
-- Database : postgres @ 178.236.185.230
-- Run      : psql -h 178.236.185.230 -U postgres -d postgres -f schemas/verification_queries.sql
-- ============================================================================


-- ============================================================================
-- PHASE 1 VERIFICATION: Schema JSON files (run from shell, not SQL)
-- ============================================================================
-- python3 -c "import json; d=json.load(open('data/master_schema_openfda.json')); print('OpenFDA fields:', len(d))"
-- python3 -c "import json; d=json.load(open('data/master_schema_dailymed.json')); print('DailyMed keys:', list(d.keys()))"
-- python3 -c "import json; d=json.load(open('data/master_schema_drugbank.json')); print('DrugBank fields:', len(d))"


-- ============================================================================
-- PHASE 2 VERIFICATION: Normalized schema files (run from shell)
-- ============================================================================
-- python3 -c "import json; d=json.load(open('data/normalized_schema.json')); print('OpenFDA categories:', list(d.keys()))"
-- python3 -c "import json; d=json.load(open('data/master_schema_dailymed_normalized.json')); print('DailyMed categories:', list(d.keys()))"
-- python3 -c "import json; d=json.load(open('data/master_schema_drugbank_normalized.json')); print('DrugBank categories:', list(d.keys()))"


-- ============================================================================
-- PHASE 3 VERIFICATION: standardized_records column population
-- ============================================================================

-- Overall population status per source
SELECT
    source,
    COUNT(*)                                                    AS total_records,
    COUNT(*) FILTER (WHERE clean_record IS NOT NULL)           AS has_clean_record,
    COUNT(*) FILTER (WHERE standardized_records IS NOT NULL)   AS has_standardized,
    ROUND(
        COUNT(*) FILTER (WHERE standardized_records IS NOT NULL)::NUMERIC
        / COUNT(*) * 100, 1
    )                                                          AS pct_complete
FROM public."DrugSourceMaster"
GROUP BY source
ORDER BY source;
-- ✅ Expected: all 4 sources at 100% (pct_complete = 100.0)

-- Verify no source is partially populated
SELECT source, COUNT(*) AS missing_standardized
FROM public."DrugSourceMaster"
WHERE standardized_records IS NULL
GROUP BY source;
-- ✅ Expected: 0 rows (every record has standardized_records)

-- Sample check: verify top-level keys look correct per source
SELECT
    source,
    jsonb_object_keys(standardized_records) AS top_key,
    COUNT(*) AS occurrences
FROM public."DrugSourceMaster"
WHERE source = 'drugbank'
GROUP BY source, top_key
ORDER BY occurrences DESC;
-- ✅ Expected: drug_info, clinical, drug_interactions (DrugBank normalized categories)


-- ============================================================================
-- PHASE 4 VERIFICATION: drugdb schema and tables exist
-- ============================================================================

-- Tables present in drugdb schema
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'drugdb'
ORDER BY table_name;
-- ✅ Expected: ingredient_interactions, ingredient_synonyms, ingredients

-- Column definitions for ingredients
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'drugdb' AND table_name = 'ingredients'
ORDER BY ordinal_position;

-- Column definitions for ingredient_synonyms
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'drugdb' AND table_name = 'ingredient_synonyms'
ORDER BY ordinal_position;

-- Column definitions for ingredient_interactions
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'drugdb' AND table_name = 'ingredient_interactions'
ORDER BY ordinal_position;


-- ============================================================================
-- PHASE 5 VERIFICATION: drugdb table row counts
-- ============================================================================

-- Row counts
SELECT 'ingredients'         AS table_name, COUNT(*) AS row_count FROM drugdb.ingredients
UNION ALL
SELECT 'ingredient_synonyms',               COUNT(*) FROM drugdb.ingredient_synonyms
UNION ALL
SELECT 'ingredient_interactions',           COUNT(*) FROM drugdb.ingredient_interactions;
-- ✅ Expected: 19,842 | 52,154 | 2,910,556

-- Migration completeness: every DrugBank source row has an ingredients entry
SELECT COUNT(*) AS missing_from_ingredients
FROM public."DrugSourceMaster" dsm
WHERE dsm.source = 'drugbank'
  AND NOT EXISTS (SELECT 1 FROM drugdb.ingredients i WHERE i.id = dsm.id);
-- ✅ Expected: 0

-- Foreign key integrity: no orphaned synonyms
SELECT COUNT(*) AS orphaned_synonyms
FROM drugdb.ingredient_synonyms s
WHERE NOT EXISTS (SELECT 1 FROM drugdb.ingredients i WHERE i.id = s.id);
-- ✅ Expected: 0

-- Foreign key integrity: no orphaned interactions
SELECT
    COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM drugdb.ingredients i WHERE i.id = di.id))          AS orphaned_by_id,
    COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM drugdb.ingredients i WHERE i.id = di.reacting_id)) AS orphaned_by_reacting_id
FROM drugdb.ingredient_interactions di;
-- ✅ Expected: 0 | 0

-- Data consistency: key fields match source JSONB
SELECT
    COUNT(*) FILTER (WHERE i.drugbank_id <> dsm.standardized_records->'drug_info'->>'drugbank_id') AS drugbank_id_mismatch,
    COUNT(*) FILTER (WHERE i.name        <> dsm.standardized_records->'drug_info'->>'name')        AS name_mismatch,
    COUNT(*) FILTER (WHERE i.unii        <> dsm.standardized_records->'drug_info'->>'unii')        AS unii_mismatch
FROM drugdb.ingredients i
JOIN public."DrugSourceMaster" dsm ON dsm.id = i.id AND dsm.source = 'drugbank';
-- ✅ Expected: 0 | 0 | 0

-- Unresolved interaction partners (known gap — 2 missing drugbank IDs)
SELECT
    interaction_item->>'drugbank_id' AS missing_drugbank_id,
    COUNT(*)                         AS referenced_count
FROM public."DrugSourceMaster" dsm
     CROSS JOIN LATERAL jsonb_array_elements(
         dsm.standardized_records->'drug_interactions'->'drug_interactions'
     ) AS interaction_item
WHERE dsm.source = 'drugbank'
  AND jsonb_typeof(dsm.standardized_records->'drug_interactions'->'drug_interactions') = 'array'
  AND NOT EXISTS (
      SELECT 1 FROM public."DrugSourceMaster" react
      WHERE react.source   = 'drugbank'
        AND react.sourceid = interaction_item->>'drugbank_id'
  )
GROUP BY interaction_item->>'drugbank_id'
ORDER BY referenced_count DESC;
-- ✅ Expected: DB09368 (542), DB24348 (58) — known gaps, no action needed

-- NULL check on critical ingredient fields
SELECT
    COUNT(*) FILTER (WHERE drugbank_id IS NULL) AS null_drugbank_id,
    COUNT(*) FILTER (WHERE name        IS NULL) AS null_name,
    COUNT(*) FILTER (WHERE unii        IS NULL) AS null_unii
FROM drugdb.ingredients;
-- ✅ Expected: 0 | 0 | 0

-- Self-referencing interactions (should never exist)
SELECT COUNT(*) AS self_interactions
FROM drugdb.ingredient_interactions
WHERE id = reacting_id;
-- ✅ Expected: 0
