-- =============================================================================
-- rebuild_drug_master_linkage_unique.sql
--
-- Rebuilds drugdb.drug_master_linkage_unique as a physical table.
-- Unique key: (generic_formulation, dosage_forms)
-- When multiple master_linkage_ids share the same combo, the record with the
-- largest combined_clean_jsonb (richest data) is selected.
-- Expected output: ~10,752 rows
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Step 1: Drop existing table
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS drugdb.drug_master_linkage_unique;


-- -----------------------------------------------------------------------------
-- Step 2: Create table
-- Unique on (generic_formulation, dosage_forms).
-- ORDER BY length DESC ensures richest JSONB wins on ties.
-- -----------------------------------------------------------------------------
CREATE TABLE drugdb.drug_master_linkage_unique AS
SELECT DISTINCT ON (d.generic_formulation, d.dosage_forms)
    d.master_linkage_id,
    d.generic_name,
    d.generic_formulation,
    d.dosage_forms,
    d.rxcui,
    dml.combined_clean_jsonb
FROM drugdb.drug d
JOIN public."DrugMasterLinkage" dml
    ON d.master_linkage_id = dml.master_linkage_id
WHERE d.generic_formulation IS NOT NULL
  AND d.dosage_forms IS NOT NULL
ORDER BY
    d.generic_formulation ASC,
    d.dosage_forms ASC,
    LENGTH(dml.combined_clean_jsonb::text) DESC;


-- -----------------------------------------------------------------------------
-- Step 3: Create indexes
-- -----------------------------------------------------------------------------

-- 3a: generic_formulation (most common filter column)
CREATE INDEX idx_dmlu_generic_formulation
    ON drugdb.drug_master_linkage_unique (generic_formulation);

-- 3b: dosage_forms (second filter column)
CREATE INDEX idx_dmlu_dosage_forms
    ON drugdb.drug_master_linkage_unique (dosage_forms);

-- 3c: composite — the natural unique key for this table
CREATE INDEX idx_dmlu_generic_formulation_dosage
    ON drugdb.drug_master_linkage_unique (generic_formulation, dosage_forms);

-- 3d: generic_name lookups
CREATE INDEX idx_dmlu_generic_name
    ON drugdb.drug_master_linkage_unique (generic_name);

-- 3e: master_linkage_id joins back to DrugMasterLinkage
CREATE INDEX idx_dmlu_master_linkage_id
    ON drugdb.drug_master_linkage_unique (master_linkage_id);

-- 3f: rxcui lookups
CREATE INDEX idx_dmlu_rxcui
    ON drugdb.drug_master_linkage_unique (rxcui);
