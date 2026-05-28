-- ============================================================
-- Phase 3 — Pre-RunPod Database Setup
-- drug_indication table + verification queries
--
-- Database: postgres
-- Schema: drugdb
-- Run this BEFORE starting the RunPod GPU job
--
-- Execution order matters:
--   1. pg_trgm extension (required before GIN index)
--   2. drug_indication table + indexes
--   3. indication_extraction_log table
--   4. verification queries
-- ============================================================


-- ============================================================
-- SECTION 1: EXTENSION
-- pg_trgm must be enabled before the GIN index on drug_indication.term
-- can be created. GIN + gin_trgm_ops requires this extension.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- ============================================================
-- SECTION 2: drug_indication TABLE + INDEXES
-- Stores one row per extracted indication per drug formulation.
-- Foreign key to drugdb.drug ensures referential integrity.
-- ============================================================

-- NOTE: formulation_id is UUID in drugdb.drug — must match here
CREATE TABLE IF NOT EXISTS drugdb.drug_indication (
    id                   SERIAL PRIMARY KEY,
    formulation_id       UUID NOT NULL
                         REFERENCES drugdb.drug(formulation_id)
                         ON DELETE CASCADE,
    term                 TEXT NOT NULL,
    icd10                TEXT,
    snomed               TEXT,
    mesh                 TEXT,
    population           TEXT DEFAULT 'any',
    line_of_therapy      TEXT DEFAULT 'unspecified',
    combination_required BOOLEAN DEFAULT FALSE,
    combination_agents   TEXT[] DEFAULT '{}',
    source_section       TEXT,
    source_excerpt       TEXT,
    source               TEXT,
    created_at           TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for fast lookups by formulation (most common join pattern)
CREATE INDEX IF NOT EXISTS idx_indication_formulation
    ON drugdb.drug_indication(formulation_id);

-- Index to support ICD-10 code filtering (e.g. find all drugs for a code)
CREATE INDEX IF NOT EXISTS idx_indication_icd10
    ON drugdb.drug_indication(icd10);

-- Index to support SNOMED CT code filtering
CREATE INDEX IF NOT EXISTS idx_indication_snomed
    ON drugdb.drug_indication(snomed);

-- GIN trigram index on term allows fast fuzzy/ILIKE searches on indication text
-- Requires pg_trgm extension (enabled in Section 1)
CREATE INDEX IF NOT EXISTS idx_indication_term
    ON drugdb.drug_indication
    USING GIN(term gin_trgm_ops);


-- ============================================================
-- SECTION 3: indication_extraction_log TABLE (CHECKPOINT)
-- Tracks which formulation_ids the LLM has already processed.
-- On crash/restart the RunPod job reads this table and skips
-- already-processed drugs to avoid duplicate work.
-- ============================================================

-- NOTE: formulation_id is UUID to match drugdb.drug
CREATE TABLE IF NOT EXISTS drugdb.indication_extraction_log (
    formulation_id    UUID PRIMARY KEY,
    status            TEXT NOT NULL DEFAULT 'done',
    rows_inserted     INTEGER DEFAULT 0,
    error_message     TEXT,
    processed_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- ============================================================
-- VERIFICATION QUERIES
-- Run after setup to confirm source data is accessible and
-- the JSON paths into combined_clean_jsonb are correct.
-- ============================================================

-- TASK 4: Count coverage of indication text across sources
SELECT
    COUNT(*) AS total_drugs,
    COUNT(CASE WHEN
        dml.combined_clean_jsonb
            -> 'openfda'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'text' IS NOT NULL
        OR
        dml.combined_clean_jsonb
            -> 'dailymed'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'content' IS NOT NULL
    THEN 1 END) AS has_indication_text,
    COUNT(CASE WHEN
        dml.combined_clean_jsonb
            -> 'openfda'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'text' IS NOT NULL
        AND
        dml.combined_clean_jsonb
            -> 'dailymed'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'content' IS NOT NULL
    THEN 1 END) AS has_both_sources,
    COUNT(CASE WHEN
        dml.combined_clean_jsonb
            -> 'openfda'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'text' IS NOT NULL
        AND
        dml.combined_clean_jsonb
            -> 'dailymed'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'content' IS NULL
    THEN 1 END) AS openfda_only,
    COUNT(CASE WHEN
        dml.combined_clean_jsonb
            -> 'openfda'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'text' IS NULL
        AND
        dml.combined_clean_jsonb
            -> 'dailymed'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'content' IS NOT NULL
    THEN 1 END) AS dailymed_only
FROM public."DrugMasterLinkage" dml
JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id;

-- TASK 5: Sample 3 rows of actual indication text to confirm JSON paths
SELECT
    d.formulation_id,
    d.generic_name,
    LEFT(
        dml.combined_clean_jsonb
            -> 'openfda'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'text',
        200
    ) AS openfda_text_sample,
    LEFT(
        dml.combined_clean_jsonb
            -> 'dailymed'
            -> 'labeling_content'
            -> 'indications_and_usage'
            ->> 'content',
        200
    ) AS dailymed_text_sample
FROM public."DrugMasterLinkage" dml
JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
WHERE
    dml.combined_clean_jsonb
        -> 'openfda'
        -> 'labeling_content'
        -> 'indications_and_usage'
        ->> 'text' IS NOT NULL
    AND
    dml.combined_clean_jsonb
        -> 'dailymed'
        -> 'labeling_content'
        -> 'indications_and_usage'
        ->> 'content' IS NOT NULL
LIMIT 3;


-- ============================================================
-- FINAL CHECKLIST
-- Seven checks that must all pass before launching RunPod.
-- ============================================================

-- Check 1: pg_trgm extension is active
SELECT extname FROM pg_extension WHERE extname = 'pg_trgm';

-- Check 2: drug_indication table exists with correct columns
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_schema = 'drugdb'
  AND table_name = 'drug_indication'
ORDER BY ordinal_position;

-- Check 3: All indexes on drug_indication were created
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'drugdb'
  AND tablename = 'drug_indication';

-- Check 4: checkpoint log table exists
SELECT COUNT(*) AS log_table_row_count
FROM drugdb.indication_extraction_log;

-- Check 5: drug table has rows to process
SELECT COUNT(*) AS total_drugs FROM drugdb.drug;

-- Check 6: DrugMasterLinkage is accessible
SELECT COUNT(*) AS total_linkage_rows
FROM public."DrugMasterLinkage";

-- Check 7: drug_indication is empty and ready
SELECT COUNT(*) AS indication_rows_currently
FROM drugdb.drug_indication;
