-- ============================================================
-- alter_drug_table_new_columns.sql
-- Adds 9 enrichment columns to drugdb.drug
-- Source: public."DrugMasterLinkage".combined_clean_jsonb
-- Join key: drugdb.drug.master_linkage_id = DrugMasterLinkage.master_linkage_id
-- SAFE: does not modify or drop any existing column
-- ============================================================

-- 1. product_type TEXT
--    From: dailymed -> identification -> drug_label ->> 'label_type'
--    " LABEL" suffix is stripped at population time (e.g. "HUMAN PRESCRIPTION DRUG LABEL" -> "HUMAN PRESCRIPTION DRUG")
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS product_type TEXT;

-- 2. routes TEXT[]
--    From: dailymed -> drug_info -> products[] -> route_of_administration
--    Distinct values across all products collected into a PostgreSQL TEXT array
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS routes TEXT[];

-- 3. mechanism_of_action TEXT
--    From: openfda -> clinical -> mechanism_of_action ->> 'text'
--    Truncated at 5000 chars with trailing "..." if longer
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS mechanism_of_action TEXT;

-- 4. record_version TEXT DEFAULT '1.0'
--    From: dailymed -> identification -> drug_label ->> 'version'
--    Falls back to '1.0' when null
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS record_version TEXT DEFAULT '1.0';

-- 5. last_ingested_at TIMESTAMPTZ DEFAULT NOW()
--    Set to the timestamp at which update_drug_new_columns.py ran — not from JSON
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS last_ingested_at TIMESTAMPTZ DEFAULT NOW();

-- 6. has_openfda BOOLEAN DEFAULT FALSE
--    True when combined_clean_jsonb -> 'openfda' exists and is not null
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS has_openfda BOOLEAN DEFAULT FALSE;

-- 7. has_dailymed BOOLEAN DEFAULT FALSE
--    True when combined_clean_jsonb -> 'dailymed' exists and is not null
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS has_dailymed BOOLEAN DEFAULT FALSE;

-- 8. has_rxnorm BOOLEAN DEFAULT FALSE
--    True when combined_clean_jsonb -> 'rxnorm' exists and is a non-empty array (length > 0)
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS has_rxnorm BOOLEAN DEFAULT FALSE;

-- 9. has_drugbank BOOLEAN DEFAULT FALSE
--    True when combined_clean_jsonb -> 'drugbank' exists and is a non-empty array (length > 0)
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS has_drugbank BOOLEAN DEFAULT FALSE;

-- ============================================================
-- Verification: confirm all 9 new columns exist
-- ============================================================
SELECT
    column_name,
    data_type,
    column_default,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name   = 'drug'
  AND column_name  IN (
      'product_type',
      'routes',
      'mechanism_of_action',
      'record_version',
      'last_ingested_at',
      'has_openfda',
      'has_dailymed',
      'has_rxnorm',
      'has_drugbank'
  )
ORDER BY column_name;
