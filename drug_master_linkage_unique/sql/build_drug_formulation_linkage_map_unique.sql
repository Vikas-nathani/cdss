-- =============================================================================
-- build_drug_formulation_linkage_map_unique.sql
--
-- Builds drugdb.drug_formulation_linkage_map_unique.
-- Purpose: clean 1-to-1 map between formulation_id and master_linkage_id,
--          resolved by joining on (master_linkage_id, generic_formulation,
--          dosage_forms).
-- Expected rows: ~10,752
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Step 1: Drop existing table
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS drugdb.drug_formulation_linkage_map_unique;


-- -----------------------------------------------------------------------------
-- Step 2: Create table
-- id       — surrogate PK (SERIAL, auto-assigned)
-- created_at — defaults to NOW(), populated explicitly in the INSERT
-- -----------------------------------------------------------------------------
CREATE TABLE drugdb.drug_formulation_linkage_map_unique (
    id                 SERIAL PRIMARY KEY,
    formulation_id     UUID                     NOT NULL,
    master_linkage_id  UUID                     NOT NULL,
    generic_name       TEXT,
    generic_formulation TEXT                    NOT NULL,
    dosage_forms       TEXT                     NOT NULL,
    rxcui              VARCHAR,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- -----------------------------------------------------------------------------
-- Step 3: Insert data
-- Join drug_master_linkage_unique → drugdb.drug on the composite key
-- (master_linkage_id, generic_formulation, dosage_forms).
-- -----------------------------------------------------------------------------
INSERT INTO drugdb.drug_formulation_linkage_map_unique
    (formulation_id, master_linkage_id, generic_name,
     generic_formulation, dosage_forms, rxcui, created_at)
SELECT
    d.formulation_id,
    dmlu.master_linkage_id,
    dmlu.generic_name,
    dmlu.generic_formulation,
    dmlu.dosage_forms,
    dmlu.rxcui,
    NOW()
FROM drugdb.drug_master_linkage_unique dmlu
JOIN drugdb.drug d
    ON  dmlu.master_linkage_id   = d.master_linkage_id
    AND dmlu.generic_formulation = d.generic_formulation
    AND dmlu.dosage_forms        = d.dosage_forms;


-- -----------------------------------------------------------------------------
-- Step 4: Unique constraint on (formulation_id, master_linkage_id)
-- Enforces one-to-one mapping integrity.
-- -----------------------------------------------------------------------------
ALTER TABLE drugdb.drug_formulation_linkage_map_unique
    ADD CONSTRAINT uq_dflmu_formulation_linkage
    UNIQUE (formulation_id, master_linkage_id);


-- -----------------------------------------------------------------------------
-- Step 5: Indexes
-- -----------------------------------------------------------------------------

-- 5a: formulation_id — primary lookup key
CREATE INDEX idx_dflmu_formulation_id
    ON drugdb.drug_formulation_linkage_map_unique (formulation_id);

-- 5b: master_linkage_id — join back to master linkage table
CREATE INDEX idx_dflmu_master_linkage_id
    ON drugdb.drug_formulation_linkage_map_unique (master_linkage_id);

-- 5c: generic_formulation — drug name queries
CREATE INDEX idx_dflmu_generic_formulation
    ON drugdb.drug_formulation_linkage_map_unique (generic_formulation);

-- 5d: dosage_forms — dosage filter queries
CREATE INDEX idx_dflmu_dosage_forms
    ON drugdb.drug_formulation_linkage_map_unique (dosage_forms);

-- 5e: rxcui — RxNorm identifier lookup
CREATE INDEX idx_dflmu_rxcui
    ON drugdb.drug_formulation_linkage_map_unique (rxcui);

-- 5f: composite — the natural unique key for formulation+dosage queries
CREATE INDEX idx_dflmu_formulation_dosage
    ON drugdb.drug_formulation_linkage_map_unique (generic_formulation, dosage_forms);


-- -----------------------------------------------------------------------------
-- Step 6: Verification queries
-- -----------------------------------------------------------------------------

-- 6a: Total rows inserted
SELECT COUNT(*) AS total_rows
FROM drugdb.drug_formulation_linkage_map_unique;

-- 6b: Duplicate formulation_ids (expected: 0)
SELECT COUNT(*) AS duplicate_formulation_ids
FROM (
    SELECT formulation_id
    FROM drugdb.drug_formulation_linkage_map_unique
    GROUP BY formulation_id
    HAVING COUNT(*) > 1
) sub;

-- 6c: Distinct master_linkage_ids (one linkage maps to many formulations)
SELECT COUNT(DISTINCT master_linkage_id) AS unique_master_linkage_ids
FROM drugdb.drug_formulation_linkage_map_unique;
