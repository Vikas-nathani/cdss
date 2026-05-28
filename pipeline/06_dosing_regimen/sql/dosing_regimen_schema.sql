-- =============================================================================
-- Phase 3 Step 3.1: Dosing Regimen Table Schema
-- CDSS Clinical Decision Support System
-- =============================================================================

-- -----------------------------------------------------------------------------
-- STEP 1: Drop existing table and create fresh
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS drugdb.dosing_regimen CASCADE;

CREATE TABLE drugdb.dosing_regimen (
    id                      SERIAL PRIMARY KEY,
    regimen_id              TEXT UNIQUE NOT NULL,
    formulation_id          UUID NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    indication              TEXT,
    age_group               TEXT DEFAULT 'any',
    age_min_years           NUMERIC,
    age_max_years           NUMERIC,
    weight_min_kg           NUMERIC,
    weight_max_kg           NUMERIC,
    sex                     TEXT DEFAULT 'any',
    pregnancy_status        TEXT DEFAULT 'any',
    renal_function          TEXT DEFAULT 'any',
    hepatic_function        TEXT DEFAULT 'any',
    route                   TEXT,
    dose_amount             TEXT,
    dose_value              NUMERIC,
    dose_unit               TEXT,
    dose_basis              TEXT,
    frequency               TEXT,
    duration                TEXT,
    max_daily_dose          TEXT,
    administration_notes    TEXT,
    adjustment_required_for TEXT[] DEFAULT '{}',
    source_section          TEXT,
    source_excerpt          TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- STEP 2: Indexes
-- -----------------------------------------------------------------------------

CREATE INDEX idx_dr_formulation ON drugdb.dosing_regimen(formulation_id);
CREATE INDEX idx_dr_indication  ON drugdb.dosing_regimen(indication);
CREATE INDEX idx_dr_population  ON drugdb.dosing_regimen(formulation_id, age_group, renal_function, hepatic_function);
CREATE INDEX idx_dr_regimen_id  ON drugdb.dosing_regimen(regimen_id);

-- -----------------------------------------------------------------------------
-- STEP 3: Column comments
-- -----------------------------------------------------------------------------

COMMENT ON TABLE drugdb.dosing_regimen IS
    'Structured dosing regimens extracted from FDA/DailyMed labeling text via LLM. '
    'Each row represents one unique combination of indication × population × route.';

COMMENT ON COLUMN drugdb.dosing_regimen.id IS
    'Auto-incrementing surrogate primary key.';

COMMENT ON COLUMN drugdb.dosing_regimen.regimen_id IS
    'Deterministic unique identifier (MD5 hex, first 16 chars) derived from '
    'formulation_id + indication + age_group + renal_function + hepatic_function '
    '+ route + sex + pregnancy_status. Guarantees idempotent inserts.';

COMMENT ON COLUMN drugdb.dosing_regimen.formulation_id IS
    'Foreign key to drugdb.drug.formulation_id. Identifies the specific drug '
    'formulation (e.g., oral tablet, IV solution) this regimen applies to.';

COMMENT ON COLUMN drugdb.dosing_regimen.indication IS
    'Clinical indication or disease state for which this dosing regimen applies. '
    'Null if the label specifies a general (non-indication-specific) regimen.';

COMMENT ON COLUMN drugdb.dosing_regimen.age_group IS
    'Target patient age group using controlled vocabulary: '
    'neonate | infant | pediatric | adolescent | adult | geriatric | any.';

COMMENT ON COLUMN drugdb.dosing_regimen.age_min_years IS
    'Minimum patient age in years (inclusive) for this regimen. Null if not specified.';

COMMENT ON COLUMN drugdb.dosing_regimen.age_max_years IS
    'Maximum patient age in years (inclusive) for this regimen. Null if not specified.';

COMMENT ON COLUMN drugdb.dosing_regimen.weight_min_kg IS
    'Minimum patient body weight in kilograms (inclusive). Null if not specified.';

COMMENT ON COLUMN drugdb.dosing_regimen.weight_max_kg IS
    'Maximum patient body weight in kilograms (inclusive). Null if not specified.';

COMMENT ON COLUMN drugdb.dosing_regimen.sex IS
    'Patient sex applicability using controlled vocabulary: any | male | female.';

COMMENT ON COLUMN drugdb.dosing_regimen.pregnancy_status IS
    'Pregnancy or lactation context using controlled vocabulary: '
    'any | pregnant | not_pregnant | lactating.';

COMMENT ON COLUMN drugdb.dosing_regimen.renal_function IS
    'Renal function tier using controlled vocabulary: '
    'any | normal | mild_impairment | moderate_impairment | severe_impairment | esrd.';

COMMENT ON COLUMN drugdb.dosing_regimen.hepatic_function IS
    'Hepatic function tier using controlled vocabulary: '
    'any | normal | mild_impairment | moderate_impairment | severe_impairment.';

COMMENT ON COLUMN drugdb.dosing_regimen.route IS
    'Route of administration as extracted from the label '
    '(e.g., oral, intravenous, subcutaneous, topical, intramuscular).';

COMMENT ON COLUMN drugdb.dosing_regimen.dose_amount IS
    'Full dose amount as a human-readable string (e.g., "500 mg", "10 mg/kg", "2 tablets"). '
    'Set to "CONTRAINDICATED" when the label explicitly contraindicates this population.';

COMMENT ON COLUMN drugdb.dosing_regimen.dose_value IS
    'Numeric dose value extracted from dose_amount for computational comparison '
    '(e.g., 500 for "500 mg"). Null when dose is not a simple numeric value.';

COMMENT ON COLUMN drugdb.dosing_regimen.dose_unit IS
    'Unit of the dose measurement (e.g., mg, mcg, mg/kg, mg/m2, mL, units).';

COMMENT ON COLUMN drugdb.dosing_regimen.dose_basis IS
    'Basis for dose calculation using controlled vocabulary: '
    'fixed | per_kg | per_m2 | titrated.';

COMMENT ON COLUMN drugdb.dosing_regimen.frequency IS
    'Dosing frequency using controlled vocabulary: '
    'QD | BID | TID | QID | q6h | q8h | q12h | weekly | biweekly | monthly | once | as_needed.';

COMMENT ON COLUMN drugdb.dosing_regimen.duration IS
    'Duration of treatment as a human-readable string '
    '(e.g., "7 days", "4 weeks", "6 months", "indefinite").';

COMMENT ON COLUMN drugdb.dosing_regimen.max_daily_dose IS
    'Maximum total daily dose as a human-readable string '
    '(e.g., "2000 mg/day", "4 g/day"). Null if not specified in the label.';

COMMENT ON COLUMN drugdb.dosing_regimen.administration_notes IS
    'Additional administration instructions from the label '
    '(e.g., "take with food", "infuse over 30 minutes", "do not crush").';

COMMENT ON COLUMN drugdb.dosing_regimen.adjustment_required_for IS
    'Array of conditions that require dose adjustment for this regimen '
    '(e.g., {renal_impairment, hepatic_impairment, elderly}). Empty array if none.';

COMMENT ON COLUMN drugdb.dosing_regimen.source_section IS
    'Labeling source from which this regimen was extracted: openfda or dailymed.';

COMMENT ON COLUMN drugdb.dosing_regimen.source_excerpt IS
    'Verbatim text excerpt from the label that supports this dosing regimen, '
    'preserved for audit and validation purposes.';

COMMENT ON COLUMN drugdb.dosing_regimen.created_at IS
    'UTC timestamp when this record was inserted into the database.';

-- -----------------------------------------------------------------------------
-- STEP 4: Verification
-- -----------------------------------------------------------------------------

SELECT
    COUNT(*)                                    AS total_rows,
    'dosing_regimen table created successfully' AS status
FROM drugdb.dosing_regimen;
