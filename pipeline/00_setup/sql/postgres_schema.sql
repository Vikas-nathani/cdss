-- ============================================================================
-- CDSS DATABASE SCHEMA — PostgreSQL 16 + pgvector
-- ============================================================================
-- Run this in order. All tables, indexes, functions, and triggers.
-- ============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS vector;           -- pgvector for RAG embeddings
CREATE EXTENSION IF NOT EXISTS pg_trgm;          -- trigram similarity for fuzzy search
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";      -- UUID generation

-- ============================================================================
-- CORE DRUG TABLES
-- ============================================================================

CREATE TABLE drugdb.drug (
    formulation_id        TEXT PRIMARY KEY,
    generic_name         TEXT NOT NULL,
    normalized_name      TEXT NOT NULL,              -- salt-stripped INN for joining
    brand_names          TEXT[] DEFAULT '{}',
    drug_class           TEXT[] DEFAULT '{}',
    atc_codes            TEXT[] DEFAULT '{}',
    product_type         TEXT,                        -- HUMAN PRESCRIPTION DRUG, HUMAN OTC DRUG, etc.
    manufacturer         TEXT,
    routes               TEXT[] DEFAULT '{}',
    dosage_forms         TEXT[] DEFAULT '{}',
    mechanism_of_action  TEXT,
    record_version       TEXT DEFAULT '1.0',
    last_ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    has_openfda          BOOLEAN DEFAULT FALSE,
    has_dailymed         BOOLEAN DEFAULT FALSE,
    has_rxnorm           BOOLEAN DEFAULT FALSE,
    has_drugbank         BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_drug_generic ON drugdb.drug(generic_name);
CREATE INDEX idx_drug_normalized ON drugdb.drug(normalized_name);
CREATE INDEX idx_drug_class ON drug USING GIN(drug_class);
CREATE INDEX idx_drug_trgm ON drug USING GIN(generic_name gin_trgm_ops);

-- -------------------------------------------------------

CREATE TABLE drugdb.drug_identifier (
        id                   SERIAL PRIMARY KEY,
            formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
                id_type              TEXT NOT NULL,               -- rxcui, ndc_product, ndc_package, unii, drugbank, upc, application_number
                    id_value             TEXT NOT NULL,
                        UNIQUE(formulation_id, id_type, id_value)
                        );

                        CREATE INDEX idx_di_lookup ON drugdb.drug_identifier(id_type, id_value);
                        CREATE INDEX idx_di_formulation ON drugdb.drug_identifier(formulation_id);
)

-- -------------------------------------------------------

CREATE TABLE active_ingredient (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    substance_name       TEXT NOT NULL,
    normalized_name      TEXT NOT NULL,
    unii                 TEXT,
    drugbank_id          TEXT,
    strength_label       TEXT,                        -- "250 mg"
    strength_value       NUMERIC,
    strength_unit        TEXT
);

CREATE INDEX idx_ai_formulation ON active_ingredient(formulation_id);
CREATE INDEX idx_ai_normalized ON active_ingredient(normalized_name);
CREATE INDEX idx_ai_drugbank ON active_ingredient(drugbank_id);

-- -------------------------------------------------------

CREATE TABLE inactive_ingredient (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    name                 TEXT NOT NULL,
    normalized_name      TEXT NOT NULL,
    unii                 TEXT,
    drugbank_id          TEXT,
    role                 TEXT                         -- excipient, colorant, preservative, etc.
);

CREATE INDEX idx_ii_formulation ON inactive_ingredient(formulation_id);

-- ============================================================================
-- STRUCTURED FACTS TABLES
-- ============================================================================

CREATE TABLE drug_indication (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    term                 TEXT NOT NULL,                -- as written in label
    icd10                TEXT,
    snomed               TEXT,
    mesh                 TEXT,
    population           TEXT DEFAULT 'any',           -- "adults", "pediatric 2-13y", "treatment-naive"
    line_of_therapy      TEXT DEFAULT 'unspecified',    -- first-line, second-line, adjunct, salvage
    combination_required BOOLEAN DEFAULT FALSE,
    combination_agents   TEXT[] DEFAULT '{}',
    source_section       TEXT,
    source_excerpt       TEXT
);

CREATE INDEX idx_indication_formulation ON drug_indication(formulation_id);
CREATE INDEX idx_indication_icd10 ON drug_indication(icd10);
CREATE INDEX idx_indication_snomed ON drug_indication(snomed);
CREATE INDEX idx_indication_term ON drug_indication USING GIN(term gin_trgm_ops);

-- -------------------------------------------------------

CREATE TABLE drug_interaction (
    id                    SERIAL PRIMARY KEY,
    interaction_id        TEXT UNIQUE NOT NULL,
    subject_formulation_id TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    subject_substance     TEXT,
    subject_substance_role TEXT DEFAULT 'unknown',     -- active_ingredient, excipient, unknown
    partner_name          TEXT NOT NULL,
    partner_rxcui         TEXT,
    partner_drugbank_id   TEXT,
    partner_drug_class    TEXT,
    severity              TEXT DEFAULT 'unknown',      -- contraindicated, major, moderate, minor, unknown
    effect_direction      TEXT,                        -- increase, decrease, no_change, unclear
    effect_on             TEXT,                        -- "AUC of partner", "Cmax of this drug", "QTc"
    magnitude             TEXT,                        -- "↑AUC 505% (393–643)"
    mechanism             TEXT,                        -- "CYP3A4 inhibition"
    clinical_management   TEXT,
    evidence_level        TEXT DEFAULT 'established',  -- established, probable, suspected, theoretical
    source                TEXT NOT NULL,               -- openfda, dailymed, drugbank, merged
    source_document_id    TEXT,
    source_section        TEXT,
    source_excerpt        TEXT
);

CREATE INDEX idx_dxi_subject ON drug_interaction(subject_formulation_id);
CREATE INDEX idx_dxi_partner_rxcui ON drug_interaction(partner_rxcui);
CREATE INDEX idx_dxi_partner_db ON drug_interaction(partner_drugbank_id);
CREATE INDEX idx_dxi_partner_name ON drug_interaction USING GIN(partner_name gin_trgm_ops);
CREATE INDEX idx_dxi_severity ON drug_interaction(severity);

-- -------------------------------------------------------

CREATE TABLE contraindication (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    kind                 TEXT NOT NULL,                -- condition, coadministered_drug, population, allergy
    term                 TEXT NOT NULL,
    rxcui                TEXT,
    drugbank_id          TEXT,
    drug_class           TEXT,
    reason               TEXT,
    severity             TEXT DEFAULT 'absolute',      -- absolute, relative
    source_section       TEXT,
    source_excerpt       TEXT
);

CREATE INDEX idx_contra_formulation ON contraindication(formulation_id);
CREATE INDEX idx_contra_term ON contraindication USING GIN(term gin_trgm_ops);

-- -------------------------------------------------------

CREATE TABLE dosing_regimen (
    id                    SERIAL PRIMARY KEY,
    regimen_id            TEXT UNIQUE NOT NULL,
    formulation_id        TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    indication            TEXT,
    -- Population filters
    age_group             TEXT DEFAULT 'any',          -- neonate, infant, pediatric, adolescent, adult, geriatric, any
    age_min_years         NUMERIC,
    age_max_years         NUMERIC,
    weight_min_kg         NUMERIC,
    weight_max_kg         NUMERIC,
    sex                   TEXT DEFAULT 'any',          -- any, male, female
    pregnancy_status      TEXT DEFAULT 'any',          -- any, pregnant, not_pregnant, lactating
    renal_function        TEXT DEFAULT 'any',          -- any, normal, mild_impairment, moderate_impairment, severe_impairment, esrd
    hepatic_function      TEXT DEFAULT 'any',          -- any, normal, mild_impairment, moderate_impairment, severe_impairment
    -- Dose
    route                 TEXT,
    dose_amount           TEXT,                        -- "1250 mg" or "45-55 mg/kg" or "CONTRAINDICATED"
    dose_value            NUMERIC,
    dose_unit             TEXT,
    dose_basis            TEXT,                        -- fixed, per_kg, per_m2, titrated
    frequency             TEXT,                        -- BID, TID, QD, q8h, etc.
    duration              TEXT,
    max_daily_dose        TEXT,
    administration_notes  TEXT,                        -- "with food", "on empty stomach"
    adjustment_required_for TEXT[] DEFAULT '{}',       -- ["coadministration with rifabutin", "moderate hepatic impairment"]
    -- Provenance
    source_section        TEXT,
    source_excerpt        TEXT
);

CREATE INDEX idx_dr_formulation ON dosing_regimen(formulation_id);
CREATE INDEX idx_dr_indication ON dosing_regimen(indication);
CREATE INDEX idx_dr_population ON dosing_regimen(formulation_id, age_group, renal_function, hepatic_function);

-- -------------------------------------------------------

CREATE TABLE population_approval (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    population           TEXT NOT NULL,               -- pediatric, adolescent, geriatric, pregnant, lactating
    status               TEXT NOT NULL,               -- approved, studied_not_approved, not_studied, contraindicated, benefit_risk
    approved_age_range   TEXT,
    pregnancy_category   TEXT,                         -- A, B, C, D, X
    has_registry         BOOLEAN DEFAULT FALSE,
    notes                TEXT,
    source_section       TEXT,
    source_excerpt       TEXT,
    UNIQUE(formulation_id, population)
);

CREATE INDEX idx_pa_formulation ON population_approval(formulation_id);
CREATE INDEX idx_pa_population ON population_approval(population, status);

-- -------------------------------------------------------

CREATE TABLE administration_timing (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    food_requirement     TEXT,                        -- with_food, empty_stomach, either, unknown
    food_details         TEXT,
    drug_separations     JSONB DEFAULT '[]',          -- [{other_drug, separation_hours, timing, reason}]
    other_timing_notes   TEXT,
    source               TEXT DEFAULT 'regex',         -- regex, llm, merged
    UNIQUE(formulation_id)
);

CREATE INDEX idx_at_formulation ON administration_timing(formulation_id);

-- -------------------------------------------------------

CREATE TABLE available_strength (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    strength_value       NUMERIC NOT NULL,
    strength_unit        TEXT NOT NULL,
    strength_label       TEXT NOT NULL,                -- "250 MG"
    rxcui                TEXT,
    dosage_form          TEXT
);

CREATE INDEX idx_as_formulation ON available_strength(formulation_id);

-- -------------------------------------------------------

CREATE TABLE adverse_event (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    term                 TEXT NOT NULL,
    meddra_pt            TEXT,
    system_organ_class   TEXT,
    frequency            TEXT,                        -- very_common, common, uncommon, rare, very_rare, unknown
    incidence_pct        NUMERIC,
    population           TEXT,
    seriousness          TEXT,                         -- serious, non_serious
    source_section       TEXT,
    source_excerpt       TEXT
);

CREATE INDEX idx_ae_formulation ON adverse_event(formulation_id);

-- -------------------------------------------------------

CREATE TABLE warning (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    warning_type         TEXT NOT NULL,                -- boxed_warning, warning, precaution
    topic                TEXT,
    text                 TEXT,
    source_section       TEXT
);

CREATE INDEX idx_warning_formulation ON warning(formulation_id);
CREATE INDEX idx_warning_type ON warning(warning_type);

-- ============================================================================
-- CLINICAL NARRATIVE + RAG TABLES
-- ============================================================================

CREATE TABLE clinical_section (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    section              TEXT NOT NULL,                -- indications_and_usage, dosage_and_administration, etc.
    text                 TEXT,
    subsections          JSONB DEFAULT '[]',           -- [{subsection_id, title, text}]
    source               TEXT,                         -- openfda, dailymed, merged
    source_document_id   TEXT,
    UNIQUE(formulation_id, section)
);

CREATE INDEX idx_cs_formulation ON clinical_section(formulation_id);
CREATE INDEX idx_cs_section ON clinical_section(section);

-- -------------------------------------------------------

CREATE TABLE label_table (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    table_id             TEXT NOT NULL,
    caption              TEXT,
    semantic_type        TEXT,                         -- dosing, interaction, adverse_event, pharmacokinetics, clinical_study, contraindication
    section              TEXT,
    headers              TEXT[],
    rows_data            JSONB DEFAULT '[]'
);

CREATE INDEX idx_lt_formulation ON label_table(formulation_id);
CREATE INDEX idx_lt_semantic ON label_table(semantic_type);

-- -------------------------------------------------------

CREATE TABLE rxnorm_formulation (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    rxcui                TEXT NOT NULL,
    tty                  TEXT,                         -- SCD, SBD, SCDC, SBDC, GPCK, BPCK
    name                 TEXT,
    kind                 TEXT,                         -- generic, brand
    dose_form            TEXT,
    strength_value       NUMERIC,
    strength_unit        TEXT,
    strength_label       TEXT,
    synonyms             TEXT[] DEFAULT '{}'
);

CREATE INDEX idx_rxnorm_formulation ON rxnorm_formulation(formulation_id);
CREATE INDEX idx_rxnorm_rxcui ON rxnorm_formulation(rxcui);

-- ============================================================================
-- RAG CHUNK TABLE (with pgvector)
-- ============================================================================

CREATE TABLE rag_chunk (
    chunk_id             TEXT PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    section              TEXT,
    subsection_id        TEXT,
    subsection_title     TEXT,
    semantic_type        TEXT NOT NULL,
    source               TEXT,
    text                 TEXT NOT NULL,
    -- Metadata for hybrid filtering
    generic_name         TEXT,
    brand_names          TEXT[],
    rxcui                TEXT[],
    drugbank_ids         TEXT[],
    manufacturer         TEXT,
    routes               TEXT[],
    dosage_forms         TEXT[],
    -- For fact chunks
    partner_name         TEXT,
    partner_drugbank_id  TEXT,
    subject_substance    TEXT,
    subject_substance_role TEXT,
    severity             TEXT,
    -- Embedding
    embedding            vector(1024)                  -- bge-large-en-v1.5 = 1024 dims
);

CREATE INDEX idx_rc_formulation ON rag_chunk(formulation_id);
CREATE INDEX idx_rc_semantic ON rag_chunk(semantic_type);
CREATE INDEX idx_rc_section ON rag_chunk(section);
-- IVFFlat index — create AFTER all embeddings are loaded
-- CREATE INDEX idx_rc_embedding ON rag_chunk USING ivfflat (embedding vector_cosine_ops) WITH (lists = 1000);

-- ============================================================================
-- INDIAN BRAND TABLES
-- ============================================================================

CREATE TABLE indian_brand (
    indian_brand_id      SERIAL PRIMARY KEY,
    brand_name           TEXT NOT NULL,
    manufacturer_india   TEXT,
    generic_name_raw     TEXT NOT NULL,
    normalized_generic_name TEXT NOT NULL,
    strength_label       TEXT,
    strength_value       NUMERIC,
    strength_unit        TEXT,
    dosage_form_raw      TEXT,
    form_canonical       TEXT,
    route                TEXT DEFAULT 'ORAL',
    pack_size            TEXT,
    schedule             TEXT,                         -- H, H1, X, G, OTC
    mrp_inr              NUMERIC,
    cdsco_approval       BOOLEAN DEFAULT TRUE,
    is_combination       BOOLEAN DEFAULT FALSE,
    formulation_id       TEXT REFERENCES drugdb.drug(formulation_id),
    match_confidence     TEXT                          -- exact, normalized, fuzzy, manual
);

CREATE INDEX idx_ib_normalized ON indian_brand(normalized_generic_name);
CREATE INDEX idx_ib_brand ON indian_brand(brand_name);
CREATE INDEX idx_ib_brand_trgm ON indian_brand USING GIN(brand_name gin_trgm_ops);
CREATE INDEX idx_ib_formulation ON indian_brand(formulation_id);
CREATE INDEX idx_ib_form ON indian_brand(form_canonical);

-- -------------------------------------------------------

CREATE TABLE indian_brand_ingredient (
    id                   SERIAL PRIMARY KEY,
    indian_brand_id      INT NOT NULL REFERENCES indian_brand(indian_brand_id) ON DELETE CASCADE,
    ingredient_index     INT NOT NULL,
    ingredient_name      TEXT NOT NULL,                -- normalized
    ingredient_strength  TEXT,
    formulation_id       TEXT REFERENCES drugdb.drug(formulation_id),
    match_confidence     TEXT
);

CREATE INDEX idx_ibi_brand ON indian_brand_ingredient(indian_brand_id);
CREATE INDEX idx_ibi_formulation ON indian_brand_ingredient(formulation_id);
CREATE INDEX idx_ibi_ingredient ON indian_brand_ingredient(ingredient_name);

-- ============================================================================
-- ENTITY RESOLVER FUNCTION
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
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- AUDIT LOG
-- ============================================================================

CREATE TABLE query_audit_log (
    id                   BIGSERIAL PRIMARY KEY,
    query_template       TEXT NOT NULL,               -- Q1, Q2, ... Q9
    request_payload      JSONB NOT NULL,
    resolved_drugs       JSONB,
    sql_results          JSONB,
    graph_results        JSONB,
    rag_chunks_used      TEXT[],
    llm_prompt           TEXT,
    llm_response         TEXT,
    indian_brands_shown  JSONB,
    response_payload     JSONB,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    response_time_ms     INT
);

CREATE INDEX idx_audit_template ON query_audit_log(query_template);
CREATE INDEX idx_audit_created ON query_audit_log(created_at);

-- ============================================================================
-- POST-LOAD: Create IVFFlat index after all embeddings are inserted
-- ============================================================================
-- Run this AFTER Phase 4 completes:
-- CREATE INDEX idx_rc_embedding ON rag_chunk 
--   USING ivfflat (embedding vector_cosine_ops) 
--   WITH (lists = 1000);
-- SET ivfflat.probes = 10;