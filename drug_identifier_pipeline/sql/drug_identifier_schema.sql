-- ============================================================
-- drug_identifier table
-- Purpose: Universal identifier lookup — maps any external ID
--          (rxcui, NDC, UNII etc.) to internal formulation_id
-- Source:  DrugMasterLinkage.combined_clean_jsonb
-- Script:  scripts/populate_drug_identifier.py
-- Logs:    logs/drug_identifier_populate_<timestamp>.log
-- id_types: rxcui, ndc_product, ndc_package, unii, upc,
--           application_number, spl_id, spl_set_id, drugbank
-- Expected rows: ~1.2-1.5 million
-- ============================================================

CREATE TABLE IF NOT EXISTS drugdb.drug_identifier (
    id             SERIAL PRIMARY KEY,
    formulation_id UUID NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    id_type        TEXT NOT NULL,
    id_value       TEXT NOT NULL,
    UNIQUE(formulation_id, id_type, id_value)
);

CREATE INDEX IF NOT EXISTS idx_di_lookup
    ON drugdb.drug_identifier(id_type, id_value);

CREATE INDEX IF NOT EXISTS idx_di_formulation
    ON drugdb.drug_identifier(formulation_id);

CREATE INDEX IF NOT EXISTS idx_di_rxcui
    ON drugdb.drug_identifier(id_value)
    WHERE id_type = 'rxcui';

CREATE INDEX IF NOT EXISTS idx_di_ndc
    ON drugdb.drug_identifier(id_value)
    WHERE id_type IN ('ndc_product', 'ndc_package');

COMMENT ON TABLE drugdb.drug_identifier IS
    'Universal identifier lookup. Maps rxcui, NDC, UNII etc. to formulation_id.';

COMMENT ON COLUMN drugdb.drug_identifier.id_type IS
    'rxcui | ndc_product | ndc_package | unii | upc | application_number | spl_id | spl_set_id | drugbank';
