-- ============================================================================
-- CDSS SCHEMA PATCH — Fixes for unmapped fields from raw JSON
-- ============================================================================
-- Run AFTER the base postgres_schema.sql
-- Addresses 4 CRITICAL + 8 IMPORTANT gaps found in field audit
-- ============================================================================


-- ============================================================================
-- CRITICAL FIX 1: partner_kind column on drug_interaction
-- Enables storing food, herb, lab, condition interactions (not just drug-drug)
-- Source: drugbank[].food_interactions (empty in sample but populated for ~30% of drugs)
-- ============================================================================

ALTER TABLE drug_interaction ADD COLUMN IF NOT EXISTS partner_kind TEXT DEFAULT 'drug';
-- partner_kind IN ('drug', 'food', 'herb', 'lab', 'condition')

COMMENT ON COLUMN drug_interaction.partner_kind IS 
  'Type of interaction partner: drug (default), food (from DrugBank food_interactions), herb, lab, condition';


-- ============================================================================
-- CRITICAL FIX 2: product_sku table for physical characteristics
-- Source: dailymed.products[].physical_characteristics (color, shape, imprint, score, size_mm)
-- Also absorbs packaging.type and packaging.quantity
-- ============================================================================

CREATE TABLE IF NOT EXISTS product_sku (
    id                SERIAL PRIMARY KEY,
    formulation_id    TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    ndc_code          TEXT,
    brand_name        TEXT,
    dosage_form       TEXT,
    -- Physical characteristics (pill identification)
    color             TEXT,
    shape             TEXT,
    imprint           TEXT,
    score             TEXT,
    size_mm           TEXT,
    -- Packaging
    packaging_type    TEXT,          -- BOTTLE, BLISTER, etc.
    packaging_qty     TEXT           -- "300 1", "10x10"
);

CREATE INDEX IF NOT EXISTS idx_sku_formulation ON product_sku(formulation_id);
CREATE INDEX IF NOT EXISTS idx_sku_imprint ON product_sku USING GIN(imprint gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sku_color_shape ON product_sku(color, shape);


-- ============================================================================
-- CRITICAL FIX 3: DrugBank text fields on ingredient tables
-- Source: drugbank[].pharmacodynamics, drugbank[].indication, drugbank[].general_function
-- These are per-ingredient clinical texts from DrugBank
-- ============================================================================

ALTER TABLE active_ingredient ADD COLUMN IF NOT EXISTS drugbank_indication TEXT;
ALTER TABLE active_ingredient ADD COLUMN IF NOT EXISTS drugbank_pharmacodynamics TEXT;
ALTER TABLE active_ingredient ADD COLUMN IF NOT EXISTS drugbank_general_function TEXT;
ALTER TABLE active_ingredient ADD COLUMN IF NOT EXISTS drugbank_classification TEXT;

ALTER TABLE inactive_ingredient ADD COLUMN IF NOT EXISTS drugbank_indication TEXT;
ALTER TABLE inactive_ingredient ADD COLUMN IF NOT EXISTS drugbank_pharmacodynamics TEXT;
ALTER TABLE inactive_ingredient ADD COLUMN IF NOT EXISTS drugbank_general_function TEXT;
ALTER TABLE inactive_ingredient ADD COLUMN IF NOT EXISTS drugbank_classification TEXT;


-- ============================================================================
-- IMPORTANT FIX 1: SPL version and effective date on drug table
-- Source: openfda.version, openfda.effective_date, dailymed.drug_label.effective_date
-- Needed for: monthly refresh change detection
-- ============================================================================

ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS spl_version TEXT;
ALTER TABLE drugdb.drug ADD COLUMN IF NOT EXISTS spl_effective_date DATE;


-- ============================================================================
-- IMPORTANT FIX 2: ingredient_synonym table
-- Source: drugbank[].synonyms (e.g., 13 synonyms for Hypromellose)
-- Needed for: entity resolver fuzzy matching on alternate names
-- ============================================================================

CREATE TABLE IF NOT EXISTS ingredient_synonym (
    id                SERIAL PRIMARY KEY,
    normalized_name   TEXT NOT NULL,       -- normalized ingredient name (salt-stripped)
    synonym           TEXT NOT NULL,
    source            TEXT DEFAULT 'drugbank',
    UNIQUE(normalized_name, synonym)
);

CREATE INDEX IF NOT EXISTS idx_syn_name ON ingredient_synonym(normalized_name);
CREATE INDEX IF NOT EXISTS idx_syn_synonym ON ingredient_synonym USING GIN(synonym gin_trgm_ops);


-- ============================================================================
-- PATCH THE ENTITY RESOLVER to search synonyms
-- ============================================================================

CREATE OR REPLACE FUNCTION resolve_drug(input_name TEXT)
RETURNS TABLE(formulation_id TEXT, match_type TEXT, matched_name TEXT) AS $$
BEGIN
    -- 1. Exact Indian brand match
    RETURN QUERY
    SELECT ib.formulation_id, 'indian_brand_exact'::TEXT, ib.brand_name
    FROM indian_brand ib
    WHERE upper(ib.brand_name) = upper(input_name)
      AND ib.formulation_id IS NOT NULL;
    IF FOUND THEN RETURN; END IF;

    -- 2. FDC ingredient decomposition
    RETURN QUERY
    SELECT ibi.formulation_id, 'indian_fdc_ingredient'::TEXT, ib.brand_name
    FROM indian_brand ib
    JOIN indian_brand_ingredient ibi USING (indian_brand_id)
    WHERE upper(ib.brand_name) = upper(input_name)
      AND ibi.formulation_id IS NOT NULL;
    IF FOUND THEN RETURN; END IF;

    -- 3. FDA generic name exact match
    RETURN QUERY
    SELECT d.formulation_id, 'fda_generic'::TEXT, d.generic_name
    FROM drugdb.drug d
    WHERE upper(d.generic_name) = upper(input_name)
       OR upper(input_name) = ANY(SELECT upper(unnest(d.brand_names)));
    IF FOUND THEN RETURN; END IF;

    -- 4. Normalized generic match (salt-stripped)
    RETURN QUERY
    SELECT d.formulation_id, 'normalized_generic'::TEXT, d.generic_name
    FROM drugdb.drug d
    WHERE d.normalized_name = upper(regexp_replace(input_name,
          '\s+(mesylate|mesilate|hydrochloride|hcl|sulfate|sulphate|sodium|potassium|calcium|acetate|maleate|fumarate|hemifumarate|tartrate|besylate|besilate|succinate|phosphate|citrate|bromide|chloride|nitrate|tosylate|trihydrate|dihydrate|monohydrate|disoproxil\s+fumarate|alafenamide|proxetil|medoxomil|axetil|pivoxil|cilexetil|stearate|palmitate|decanoate|valerate|propionate|dipropionate|furoate)\s*$',
          '', 'i'));
    IF FOUND THEN RETURN; END IF;

    -- 4b. NEW: DrugBank synonym match
    RETURN QUERY
    SELECT DISTINCT ai.formulation_id, 'synonym_match'::TEXT, syn.synonym
    FROM ingredient_synonym syn
    JOIN active_ingredient ai ON ai.normalized_name = syn.normalized_name
    WHERE upper(syn.synonym) = upper(input_name)
      AND ai.formulation_id IS NOT NULL;
    IF FOUND THEN RETURN; END IF;

    -- 5. Fuzzy Indian brand (trigram > 0.6)
    RETURN QUERY
    SELECT ib.formulation_id, 'fuzzy_brand'::TEXT, ib.brand_name
    FROM indian_brand ib
    WHERE similarity(upper(ib.brand_name), upper(input_name)) > 0.6
      AND ib.formulation_id IS NOT NULL
    ORDER BY similarity(upper(ib.brand_name), upper(input_name)) DESC
    LIMIT 3;
    IF FOUND THEN RETURN; END IF;

    -- 6. Fuzzy generic (trigram > 0.5)
    RETURN QUERY
    SELECT d.formulation_id, 'fuzzy_generic'::TEXT, d.generic_name
    FROM drugdb.drug d
    WHERE similarity(upper(d.generic_name), upper(input_name)) > 0.5
    ORDER BY similarity(upper(d.generic_name), upper(input_name)) DESC
    LIMIT 3;
    
    -- 7. NEW: Fuzzy synonym match
    IF NOT FOUND THEN
        RETURN QUERY
        SELECT DISTINCT ai.formulation_id, 'fuzzy_synonym'::TEXT, syn.synonym
        FROM ingredient_synonym syn
        JOIN active_ingredient ai ON ai.normalized_name = syn.normalized_name
        WHERE similarity(upper(syn.synonym), upper(input_name)) > 0.5
          AND ai.formulation_id IS NOT NULL
        ORDER BY similarity(upper(syn.synonym), upper(input_name)) DESC
        LIMIT 3;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- ADDITIONAL CLINICAL SECTIONS to store in transform_to_unified.py
-- No schema change needed — just add these section names to the INSERT list:
--   'drug_description'                    ← openfda.drug_description
--   'dosage_forms_and_strengths'           ← openfda.dosage_forms_and_strengths
--   'how_supplied_storage'                 ← openfda.how_supplied_storage
--   'carcinogenesis_mutagenesis_fertility' ← openfda.carcinogenesis_and_mutagenesis_...
-- ============================================================================

-- List of sections that transform_to_unified.py should now INSERT into clinical_section:
-- (This is a comment for the developer — no SQL change needed)
--
-- EXISTING (already handled):
--   indications_and_usage, dosage_and_administration, contraindications,
--   warnings_and_precautions, adverse_reactions, drug_interactions,
--   use_in_specific_populations, pediatric_use, geriatric_use,
--   use_in_pregnancy, clinical_pharmacology, pharmacokinetics,
--   pharmacodynamics, mechanism_of_action, microbiology, overdosage,
--   nonclinical_toxicology, clinical_studies, information_for_patients,
--   spl_patient_package_insert
--
-- NEW (add to the section map):
--   drug_description                      ← openfda.drug_description
--   dosage_forms_and_strengths            ← openfda.dosage_forms_and_strengths
--   how_supplied_storage                  ← openfda.how_supplied_storage OR openfda.storage_and_handling
--   carcinogenesis_mutagenesis_fertility  ← openfda.carcinogenesis_and_mutagenesis_and_impairment_of_fertility