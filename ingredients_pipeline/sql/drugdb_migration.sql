-- ============================================================================
-- MIGRATION: public.DrugSourceMaster → drugdb ingredient tables
-- Database   : postgres
-- Source     : public.DrugSourceMaster  WHERE source = 'drugbank'  (~19,842 rows)
-- Targets    : drugdb.ingredients, drugdb.ingredient_synonyms, drugdb.ingredient_interactions
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- Step 1: drugdb.ingredients
-- One row per DrugBank entry. NULL fields stay NULL.
-- ---------------------------------------------------------------------------
INSERT INTO drugdb.ingredients (
    id,
    drugbank_id,
    unii,
    rxcui,
    name,
    indications,
    general_function,
    type,
    pharmacodynamics,
    classification_description,
    food_interactions,
    created_by,
    created_at,
    updated_at
)
SELECT
    dsm.id,
    dsm.standardized_records -> 'drug_info' ->> 'drugbank_id',
    dsm.standardized_records -> 'drug_info' ->> 'unii',
    NULL,                                                           -- rxcui not in DrugBank source
    dsm.standardized_records -> 'drug_info' ->> 'name',
    dsm.standardized_records -> 'clinical'  ->> 'indication',
    dsm.standardized_records -> 'clinical'  ->> 'general_function',
    NULL,                                                           -- type not in DrugBank source
    dsm.standardized_records -> 'clinical'  ->> 'pharmacodynamics',
    dsm.standardized_records -> 'drug_info' ->> 'classification_description',
    -- Convert food_interactions array to JSON text; NULL when absent or empty
    CASE
        WHEN jsonb_typeof(dsm.standardized_records -> 'drug_interactions' -> 'food_interactions') = 'array'
             AND jsonb_array_length(dsm.standardized_records -> 'drug_interactions' -> 'food_interactions') > 0
        THEN (dsm.standardized_records -> 'drug_interactions' -> 'food_interactions')::TEXT
        ELSE NULL
    END,
    'admin',
    NOW(),
    NOW()
FROM public."DrugSourceMaster" dsm
WHERE dsm.source = 'drugbank'
ON CONFLICT (id) DO NOTHING;


-- ---------------------------------------------------------------------------
-- Step 2: drugdb.ingredient_synonyms
-- Expands the synonyms JSON array — one row per synonym per ingredient.
-- Skips drugs with no synonyms or an empty array.
-- ---------------------------------------------------------------------------
INSERT INTO drugdb.ingredient_synonyms (
    id,
    synonym,
    created_by,
    created_at,
    updated_at
)
SELECT
    dsm.id,
    syn_text,
    'admin',
    NOW(),
    NOW()
FROM public."DrugSourceMaster" dsm
     CROSS JOIN LATERAL jsonb_array_elements_text(
         dsm.standardized_records -> 'drug_info' -> 'synonyms'
     ) AS syn_text
WHERE dsm.source = 'drugbank'
  AND jsonb_typeof(dsm.standardized_records -> 'drug_info' -> 'synonyms') = 'array'
  AND jsonb_array_length(dsm.standardized_records -> 'drug_info' -> 'synonyms') > 0
  AND syn_text IS NOT NULL
  AND trim(syn_text) <> ''
ON CONFLICT (id, synonym) DO NOTHING;


-- ---------------------------------------------------------------------------
-- Step 3: drugdb.ingredient_interactions
-- Expands the drug_interactions JSON array — one row per interaction pair.
-- Resolves the reacting drug by matching its drugbank_id against
-- DrugSourceMaster.source_id. Rows with no matching counterpart are
-- silently skipped (INNER JOIN eliminates them).
-- ---------------------------------------------------------------------------
INSERT INTO drugdb.ingredient_interactions (
    id,
    reacting_id,
    description,
    created_by,
    created_at,
    updated_at
)
SELECT
    dsm.id,
    react.id,
    interaction_item ->> 'description',
    'admin',
    NOW(),
    NOW()
FROM public."DrugSourceMaster" dsm
     CROSS JOIN LATERAL jsonb_array_elements(
         dsm.standardized_records -> 'drug_interactions' -> 'drug_interactions'
     ) AS interaction_item
     JOIN public."DrugSourceMaster" react
       ON react.source    = 'drugbank'
      AND react.sourceid = interaction_item ->> 'drugbank_id'
WHERE dsm.source = 'drugbank'
  AND jsonb_typeof(dsm.standardized_records -> 'drug_interactions' -> 'drug_interactions') = 'array'
  AND jsonb_array_length(dsm.standardized_records -> 'drug_interactions' -> 'drug_interactions') > 0
ON CONFLICT (id, reacting_id) DO NOTHING;


COMMIT;


-- ============================================================================
-- Verification (run separately after migration)
-- ============================================================================
-- SELECT 'ingredients'         AS tbl, COUNT(*) FROM drugdb.ingredients;
-- SELECT 'ingredient_synonyms' AS tbl, COUNT(*) FROM drugdb.ingredient_synonyms;
-- SELECT 'ingredient_interactions' AS tbl, COUNT(*) FROM drugdb.ingredient_interactions;
