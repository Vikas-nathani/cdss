-- add_rxnorm_columns.sql
-- Adds rxnorm_generic_formulation and rxcui columns to drugdb.drug,
-- then creates indexes for fast downstream lookups.
--
-- Safe to run multiple times (IF NOT EXISTS guards on everything).
-- Run against the 'postgres' database (same DB as drugdb.drug).

BEGIN;

-- Phase 1: Add columns
ALTER TABLE drugdb.drug
    ADD COLUMN IF NOT EXISTS rxnorm_generic_formulation TEXT,
    ADD COLUMN IF NOT EXISTS rxcui                      VARCHAR(50);

-- Phase 2: Create indexes
CREATE INDEX IF NOT EXISTS idx_drug_rxnorm_formulation
    ON drugdb.drug (rxnorm_generic_formulation);

CREATE INDEX IF NOT EXISTS idx_drug_rxcui
    ON drugdb.drug (rxcui);

COMMIT;

-- Verification
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name   = 'drug'
  AND column_name  IN ('rxnorm_generic_formulation', 'rxcui')
ORDER BY column_name;

SELECT
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename  = 'drug'
  AND indexname  IN ('idx_drug_rxnorm_formulation', 'idx_drug_rxcui')
ORDER BY indexname;
