-- ============================================================================
-- DATA QUALITY ANALYSIS: drugdb migration from public.DrugSourceMaster
-- Generated: 2026-04-30
-- Database : postgres @ 178.236.185.230
-- ============================================================================
-- Run each section independently or as a whole. All queries are read-only.
-- ============================================================================


-- ============================================================================
-- SECTION 1: RECORD COUNTS  [INFO]
-- ============================================================================
-- Expected: ingredients = source DrugBank rows (19,842)
-- Actual  : PASS — all 19,842 source rows migrated
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 'source_drugbank'        AS label, COUNT(*) AS cnt FROM public."DrugSourceMaster" WHERE source = 'drugbank'
UNION ALL
SELECT 'ingredients',                     COUNT(*)        FROM drugdb.ingredients
UNION ALL
SELECT 'ingredient_synonyms',             COUNT(*)        FROM drugdb.ingredient_synonyms
UNION ALL
SELECT 'ingredient_interactions',         COUNT(*)        FROM drugdb.ingredient_interactions;
-- Results (2026-04-30):
--   source_drugbank      : 19,842
--   ingredients          : 19,842  ✓
--   ingredient_synonyms  : 52,154
--   ingredient_interactions : 2,910,556


-- ============================================================================
-- SECTION 2: MIGRATION COMPLETENESS  [CRITICAL]
-- ============================================================================
-- Expected: 0 source rows missing from ingredients
-- Actual  : PASS — 0 missing
-- ─────────────────────────────────────────────────────────────────────────────
SELECT COUNT(*) AS missing_from_ingredients
FROM public."DrugSourceMaster" dsm
WHERE dsm.source = 'drugbank'
  AND NOT EXISTS (SELECT 1 FROM drugdb.ingredients i WHERE i.id = dsm.id);


-- ============================================================================
-- SECTION 3: NULL ANALYSIS — CRITICAL FIELDS  [CRITICAL / WARNING]
-- ============================================================================
-- Critical (should never be NULL): drugbank_id, name, unii
-- Warning  (expected NULLs are fine): food_interactions (only 1,453 / 19,842 populated)
-- ─────────────────────────────────────────────────────────────────────────────
-- Results (2026-04-30):
--   null_drugbank_id       : 0   ✓
--   null_name              : 0   ✓
--   null_unii              : 0   ✓
--   null_indications       : 0   ✓
--   null_general_function  : 0   ✓
--   null_pharmacodynamics  : 0   ✓
--   null_classification    : 0   ✓
--   null_food_interactions : 18,389  (expected — most drugs have no food interactions)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                               AS total,
    COUNT(*) FILTER (WHERE drugbank_id         IS NULL)   AS null_drugbank_id,
    COUNT(*) FILTER (WHERE name                IS NULL)   AS null_name,
    COUNT(*) FILTER (WHERE unii                IS NULL)   AS null_unii,
    COUNT(*) FILTER (WHERE indications         IS NULL)   AS null_indications,
    COUNT(*) FILTER (WHERE general_function    IS NULL)   AS null_general_function,
    COUNT(*) FILTER (WHERE pharmacodynamics    IS NULL)   AS null_pharmacodynamics,
    COUNT(*) FILTER (WHERE classification_description IS NULL) AS null_classification,
    COUNT(*) FILTER (WHERE food_interactions   IS NULL)   AS null_food_interactions
FROM drugdb.ingredients;


-- ============================================================================
-- SECTION 4: FOREIGN KEY INTEGRITY  [CRITICAL]
-- ============================================================================
-- Expected: 0 orphans in both child tables
-- Actual  : PASS — 0 orphans
-- ─────────────────────────────────────────────────────────────────────────────
-- 4a. Orphaned synonym rows
SELECT COUNT(*) AS orphaned_synonyms
FROM drugdb.ingredient_synonyms s
WHERE NOT EXISTS (SELECT 1 FROM drugdb.ingredients i WHERE i.id = s.id);

-- 4b. Orphaned interaction rows (by id or reacting_id)
SELECT
    COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM drugdb.ingredients i WHERE i.id = di.id))           AS orphaned_by_id,
    COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM drugdb.ingredients i WHERE i.id = di.reacting_id))  AS orphaned_by_reacting_id
FROM drugdb.ingredient_interactions di;


-- ============================================================================
-- SECTION 5: DATA CONSISTENCY — SOURCE vs TARGET  [CRITICAL]
-- ============================================================================
-- Verifies key fields copied correctly from JSONB to flat columns.
-- Expected: 0 mismatches for drugbank_id, name, unii
-- Actual  : PASS — 0 mismatches
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    COUNT(*) FILTER (WHERE i.drugbank_id <> dsm.standardized_records->'drug_info'->>'drugbank_id') AS drugbank_id_mismatch,
    COUNT(*) FILTER (WHERE i.name        <> dsm.standardized_records->'drug_info'->>'name')        AS name_mismatch,
    COUNT(*) FILTER (WHERE i.unii        <> dsm.standardized_records->'drug_info'->>'unii')        AS unii_mismatch
FROM drugdb.ingredients i
JOIN public."DrugSourceMaster" dsm ON dsm.id = i.id AND dsm.source = 'drugbank';


-- ============================================================================
-- SECTION 6: INTERACTION COUNT — SOURCE vs MIGRATED  [WARNING]
-- ============================================================================
-- Source JSON total entries : 2,911,156
-- Migrated rows             : 2,910,556
-- Dropped (unresolved)      :       600  → only 2 distinct drugbank_ids (DB09368, DB24348)
-- Drugs with interactions   :     4,631  of 19,842
-- ─────────────────────────────────────────────────────────────────────────────
WITH source_counts AS (
    SELECT
        SUM(jsonb_array_length(standardized_records->'drug_interactions'->'drug_interactions')) AS src_total,
        COUNT(*) FILTER (
            WHERE jsonb_typeof(standardized_records->'drug_interactions'->'drug_interactions') = 'array'
        ) AS drugs_with_interactions
    FROM public."DrugSourceMaster"
    WHERE source = 'drugbank'
      AND jsonb_typeof(standardized_records->'drug_interactions'->'drug_interactions') = 'array'
      AND jsonb_array_length(standardized_records->'drug_interactions'->'drug_interactions') > 0
),
migrated_counts AS (
    SELECT COUNT(*) AS migrated_total FROM drugdb.ingredient_interactions
)
SELECT
    sc.src_total        AS source_interaction_entries,
    mc.migrated_total   AS migrated_rows,
    sc.src_total - mc.migrated_total AS dropped_due_to_unresolved_reacting_id,
    sc.drugs_with_interactions
FROM source_counts sc, migrated_counts mc;


-- ============================================================================
-- SECTION 7: UNRESOLVED INTERACTION PARTNERS  [WARNING]
-- ============================================================================
-- Only 2 drugbank_ids referenced in interactions are absent from DrugSourceMaster.
-- DB09368 : referenced 542 times — likely a DrugBank DB-only entry (no full record)
-- DB24348 : referenced  58 times — same situation
-- ACTION  : These 600 rows were correctly excluded. No fix needed unless
--           DrugBank full records for these IDs are acquired separately.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    interaction_item->>'drugbank_id'  AS missing_drugbank_id,
    COUNT(*)                          AS referenced_count
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


-- ============================================================================
-- SECTION 8: SYNONYM ANALYSIS  [INFO / WARNING]
-- ============================================================================
-- Source synonym entries   : 52,154 (zero dropped — all match migrated count)
-- Drugs with synonyms      : 18,160 / 19,842
-- Drugs with 0 synonyms    :  1,682  (8.5% — expected for simple/novel drugs)
-- Duplicate (id, synonym)  :      0  ✓
-- Avg synonyms/ingredient  :   2.63  |  Median: 1  |  Max: 65 (Castor oil)
-- ─────────────────────────────────────────────────────────────────────────────
-- 8a. Distribution
SELECT
    MIN(syn_count)                                                  AS min_synonyms,
    ROUND(AVG(syn_count), 2)                                        AS avg_synonyms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY syn_count)          AS median_synonyms,
    MAX(syn_count)                                                   AS max_synonyms,
    COUNT(*) FILTER (WHERE syn_count = 0)                           AS zero_synonym_count,
    COUNT(*) FILTER (WHERE syn_count >= 10)                         AS ten_plus_synonyms
FROM (
    SELECT i.id, COUNT(s.synonym) AS syn_count
    FROM drugdb.ingredients i
    LEFT JOIN drugdb.ingredient_synonyms s ON s.id = i.id
    GROUP BY i.id
) x;

-- 8b. Top 10 by synonym count
SELECT i.name, i.drugbank_id, COUNT(s.synonym) AS syn_count
FROM drugdb.ingredients i
JOIN drugdb.ingredient_synonyms s ON s.id = i.id
GROUP BY i.id, i.name, i.drugbank_id
ORDER BY syn_count DESC
LIMIT 10;

-- 8c. Ingredients with 0 synonyms (spot-check sample)
SELECT i.name, i.drugbank_id
FROM drugdb.ingredients i
WHERE NOT EXISTS (SELECT 1 FROM drugdb.ingredient_synonyms s WHERE s.id = i.id)
ORDER BY i.name
LIMIT 20;


-- ============================================================================
-- SECTION 9: INTERACTION ANALYSIS  [INFO / WARNING]
-- ============================================================================
-- Self-referencing (id = reacting_id)  :         0  ✓
-- Duplicate (id, reacting_id) pairs    :         0  ✓
-- Ingredients with 0 interactions      :    15,211  (76.7% — expected; most DrugBank
--                                             entries are herbal/nutraceutical/biotech)
-- Avg interactions/ingredient          :   146.69   |  Median: 0  |  Max: 2,637 (Clozapine)
-- Bidirectional pairs                  : 2,910,556 / 2,910,556  (100% — all fully symmetric)
-- ─────────────────────────────────────────────────────────────────────────────
-- 9a. Summary stats
SELECT
    COUNT(*) FILTER (WHERE id = reacting_id)                        AS self_interactions,
    (SELECT COUNT(*) FROM (
        SELECT id, reacting_id FROM drugdb.ingredient_interactions
        GROUP BY id, reacting_id HAVING COUNT(*) > 1
    ) x)                                                             AS duplicate_pairs,
    (SELECT COUNT(*) FROM drugdb.ingredients i
     WHERE NOT EXISTS (SELECT 1 FROM drugdb.ingredient_interactions di WHERE di.id = i.id)
       AND NOT EXISTS (SELECT 1 FROM drugdb.ingredient_interactions di WHERE di.reacting_id = i.id)
    )                                                                AS no_interaction_ingredients
FROM drugdb.ingredient_interactions;

-- 9b. Distribution
SELECT
    MIN(ix_count)                                                    AS min_interactions,
    ROUND(AVG(ix_count), 2)                                          AS avg_interactions,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ix_count)            AS median_interactions,
    MAX(ix_count)                                                     AS max_interactions,
    COUNT(*) FILTER (WHERE ix_count = 0)                             AS zero_interaction_count,
    COUNT(*) FILTER (WHERE ix_count >= 100)                          AS hundred_plus
FROM (
    SELECT i.id, COUNT(di.reacting_id) AS ix_count
    FROM drugdb.ingredients i
    LEFT JOIN drugdb.ingredient_interactions di ON di.id = i.id
    GROUP BY i.id
) x;

-- 9c. Top 10 most-interacting ingredients
SELECT i.name, i.drugbank_id, COUNT(di.reacting_id) AS interaction_count
FROM drugdb.ingredients i
JOIN drugdb.ingredient_interactions di ON di.id = i.id
GROUP BY i.id, i.name, i.drugbank_id
ORDER BY interaction_count DESC
LIMIT 10;


-- ============================================================================
-- SECTION 10: FORMAT VALIDATION  [INFO]
-- ============================================================================
-- food_interactions JSON validity : 0 invalid  ✓
-- id column types                 : UUID (all three tables)  ✓
-- created_at / updated_at NULLs   : 0  ✓
-- Migration timestamp             : 2026-04-30 10:50:24 UTC
-- ─────────────────────────────────────────────────────────────────────────────
-- 10a. food_interactions valid JSON
SELECT COUNT(*) AS non_null_food_interactions,
       COUNT(*) FILTER (WHERE food_interactions::jsonb IS NULL) AS invalid_json
FROM drugdb.ingredients
WHERE food_interactions IS NOT NULL;

-- 10b. Timestamp range
SELECT
    COUNT(*) FILTER (WHERE created_at IS NULL) AS null_created_at,
    COUNT(*) FILTER (WHERE updated_at IS NULL) AS null_updated_at,
    MIN(created_at)                             AS earliest,
    MAX(created_at)                             AS latest
FROM drugdb.ingredients;
