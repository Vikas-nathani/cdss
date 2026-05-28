-- =============================================================================
-- setup_schema.sql — Sibling Enrichment Pass: schema setup
--
-- Run once before executing enrich_sibling_fill.py.
-- Safe to re-run (all DDL uses IF NOT EXISTS / IF NOT EXISTS guards).
--
-- Tables written:
--   drugdb.drug_master_linkage_unique   → ADD COLUMN unified_json_enriched JSONB
--   drugdb.drug_master_linkage_enrichment_audit  → CREATE (new)
--
-- Tables read (unchanged):
--   public."DrugMasterLinkage"
--   drugdb.drug
--   drugdb.drug_master_linkage_unique
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. New enriched-JSON column on drug_master_linkage_unique
--    Stores the gap-filled copy of combined_clean_jsonb produced by each run.
--    Source column (combined_clean_jsonb) is never modified.
-- -----------------------------------------------------------------------------
ALTER TABLE drugdb.drug_master_linkage_unique
    ADD COLUMN IF NOT EXISTS unified_json_enriched JSONB;

COMMENT ON COLUMN drugdb.drug_master_linkage_unique.unified_json_enriched IS
    'Gap-filled copy of combined_clean_jsonb produced by the sibling enrichment pass '
    '(enrich_sibling_fill.py). Null fields are filled from sibling mLIds sharing the '
    'same (generic_formulation, dosage_forms). Updated on each full-run; dry-run leaves '
    'this column unchanged.';


-- -----------------------------------------------------------------------------
-- 2. Audit table — one row per field per record per run
--    Captures every fill, skip, and error across all runs.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS drugdb.drug_master_linkage_enrichment_audit (

    -- Surrogate primary key
    audit_id                BIGSERIAL       PRIMARY KEY,

    -- Identifies the pipeline run; all rows from one invocation share the same UUID
    run_id                  UUID            NOT NULL,

    -- 'dry_run'  → enriched JSON NOT written to drug_master_linkage_unique
    -- 'full_run' → enriched JSON IS written to drug_master_linkage_unique
    run_mode                TEXT            NOT NULL
                                CHECK (run_mode IN ('dry_run', 'full_run')),

    -- The chosen master_linkage_id from drug_master_linkage_unique being enriched
    target_mlid             TEXT            NOT NULL,

    -- Identifying columns of the target row (for reporting without joins)
    generic_formulation     TEXT,
    dosage_form             TEXT,

    -- Dot-separated path to the field within combined_clean_jsonb
    -- e.g. 'pharmacokinetics.half_life', 'openfda.drug_info.manufacturer_name'
    field_path              TEXT            NOT NULL,

    -- Value in the target JSON before enrichment (null/empty string/empty array/empty object)
    original_value          JSONB,

    -- Value written into the enriched JSON (null when status != 'filled')
    filled_value            JSONB,

    -- Serialized-text lengths for quick comparison queries
    original_value_length   INT,
    filled_value_length     INT,

    -- The sibling mLId whose value won the longest-text selection
    -- Empty string when status is 'skipped_no_sibling_value' or 'error'
    source_sibling_mlid     TEXT            NOT NULL,

    -- Number of siblings that had a non-null value for this field (before tie-break)
    sibling_count           INT,

    -- Outcome of the fill attempt for this field
    status                  TEXT            NOT NULL
                                CHECK (status IN (
                                    'filled',
                                    'skipped_no_sibling_value',
                                    'error'
                                )),

    -- Populated only when status = 'error'
    error_message           TEXT,

    created_at              TIMESTAMPTZ     DEFAULT NOW()

);

COMMENT ON TABLE drugdb.drug_master_linkage_enrichment_audit IS
    'Audit trail for the sibling enrichment pass. One row per field per record per run. '
    'Dry-run rows (run_mode=''dry_run'') are written even though the enriched column is '
    'not updated, allowing verification before committing a full-run.';


-- -----------------------------------------------------------------------------
-- 3. Server-side fill function
--    Applies an arbitrary set of (dot-path → value) fills to a JSONB document.
--    Called by the single end-of-run UPDATE; avoids sending enriched JSONBs
--    over the network from Python.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION drugdb.apply_json_fills(
    base_json  JSONB,
    fills      JSONB   -- {"dot.separated.path": value, ...}
) RETURNS JSONB AS $$
DECLARE
    result     JSONB   := base_json;
    path_key   TEXT;
    fill_val   JSONB;
BEGIN
    FOR path_key, fill_val IN SELECT key, value FROM jsonb_each(fills) LOOP
        result := jsonb_set(result, string_to_array(path_key, '.'), fill_val, true);
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION drugdb.apply_json_fills(JSONB, JSONB) IS
    'Applies a map of {dot.path: value} fills to a JSONB document using jsonb_set. '
    'Used by the sibling enrichment pass to write unified_json_enriched server-side, '
    'avoiding large JSONB transfers from Python.';


-- -----------------------------------------------------------------------------
-- 4. Indexes on the audit table
-- -----------------------------------------------------------------------------

-- Run-level queries (verify all rows for a run, compare runs)
CREATE INDEX IF NOT EXISTS idx_dmlea_run_id
    ON drugdb.drug_master_linkage_enrichment_audit (run_id);

-- Record-level audit trail: all fills for a specific mLId
CREATE INDEX IF NOT EXISTS idx_dmlea_target_mlid
    ON drugdb.drug_master_linkage_enrichment_audit (target_mlid);

-- Field-level analysis: which fields are most frequently null
CREATE INDEX IF NOT EXISTS idx_dmlea_field_path
    ON drugdb.drug_master_linkage_enrichment_audit (field_path);

-- Time-ordered operational queries by mode
CREATE INDEX IF NOT EXISTS idx_dmlea_run_mode_created_at
    ON drugdb.drug_master_linkage_enrichment_audit (run_mode, created_at);


-- -----------------------------------------------------------------------------
-- 4. Verification
-- -----------------------------------------------------------------------------

-- Check that the new column exists
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'drugdb'
  AND table_name   = 'drug_master_linkage_unique'
  AND column_name  = 'unified_json_enriched';

-- Check that the audit table exists with expected column count
SELECT COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema = 'drugdb'
  AND table_name   = 'drug_master_linkage_enrichment_audit';
