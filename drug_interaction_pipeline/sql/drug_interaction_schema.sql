-- drug_interaction_schema.sql
--
-- Creates drugdb.drug_interaction — formulation-level pairwise interaction pairs
-- resolved from drugdb.ingredient_interactions via drugdb.drug_ingredient_mapping.
--
-- Run after:
--   1. schemas/ingredient_schema.sql        (drugdb.ingredients + ingredient_interactions)
--   2. schemas/create_drug_table.sql        (drugdb.drug)
--   3. scripts/populate_drug_ingredient_mapping.py  (drugdb.drug_ingredient_mapping)
--
-- Populated by:
--   scripts/populate_drug_interaction.py
--
-- LLM enrichment (pending):
--   severity and mechanism columns are NULL after initial population.
--   They are filled by the LLM enrichment phase using source_excerpt as input.

CREATE TABLE IF NOT EXISTS drugdb.drug_interaction (
    id                      SERIAL PRIMARY KEY,
    interaction_id          TEXT UNIQUE NOT NULL,
    subject_formulation_id  TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    partner_formulation_id  TEXT NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    severity                TEXT DEFAULT 'unknown',
    mechanism               TEXT,
    evidence_level          TEXT DEFAULT 'established',
    source_excerpt          TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dxi_subject        ON drugdb.drug_interaction (subject_formulation_id);
CREATE INDEX IF NOT EXISTS idx_dxi_partner        ON drugdb.drug_interaction (partner_formulation_id);
CREATE INDEX IF NOT EXISTS idx_dxi_severity       ON drugdb.drug_interaction (severity);
CREATE INDEX IF NOT EXISTS idx_dxi_interaction_id ON drugdb.drug_interaction (interaction_id);

-- Table comment
COMMENT ON TABLE drugdb.drug_interaction IS
'Pairwise drug-drug interaction table at the formulation level. Each row represents '
'a directional interaction between two drug formulations (subject → partner). '
'Derived from drugdb.ingredient_interactions by resolving ingredient UUIDs to '
'formulation IDs via drugdb.drug_ingredient_mapping. '
'Populated by scripts/populate_drug_interaction.py. '
'severity and mechanism columns are left NULL by the population script and are '
'filled during the LLM enrichment phase.';

-- Column comments
COMMENT ON COLUMN drugdb.drug_interaction.id IS
'Auto-increment surrogate primary key.';

COMMENT ON COLUMN drugdb.drug_interaction.interaction_id IS
'Synthetic deduplication key: subject_formulation_id || ''_'' || partner_formulation_id. '
'UNIQUE constraint prevents duplicate pairs on re-runs. '
'Set by scripts/populate_drug_interaction.py.';

COMMENT ON COLUMN drugdb.drug_interaction.subject_formulation_id IS
'The drug formulation being checked (the "object" drug). '
'FK to drugdb.drug.formulation_id. '
'Resolved from drugdb.ingredient_interactions.id (subject ingredient) '
'via drugdb.drug_ingredient_mapping.';

COMMENT ON COLUMN drugdb.drug_interaction.partner_formulation_id IS
'The drug formulation it interacts with (the "precipitant" drug). '
'FK to drugdb.drug.formulation_id. '
'Resolved from drugdb.ingredient_interactions.reacting_id (partner ingredient) '
'via drugdb.drug_ingredient_mapping.';

COMMENT ON COLUMN drugdb.drug_interaction.severity IS
'Interaction severity: major, moderate, minor, or unknown. '
'DEFAULT ''unknown'' after initial population. '
'Populated by the LLM enrichment phase using source_excerpt as input.';

COMMENT ON COLUMN drugdb.drug_interaction.mechanism IS
'Mechanistic explanation of why the interaction occurs. '
'NULL after initial population. '
'Populated by the LLM enrichment phase using source_excerpt as input.';

COMMENT ON COLUMN drugdb.drug_interaction.evidence_level IS
'Confidence level of the interaction. DEFAULT ''established'' (DrugBank interactions '
'are curated and considered established). Can be overridden per row if per-interaction '
'evidence metadata is available from DrugSourceMaster.';

COMMENT ON COLUMN drugdb.drug_interaction.source_excerpt IS
'Raw interaction description text from DrugBank, propagated from '
'drugdb.ingredient_interactions.description. '
'Used as LLM input for severity and mechanism extraction.';

COMMENT ON COLUMN drugdb.drug_interaction.created_at IS
'Timestamp when the row was first inserted. Auto-filled by DEFAULT NOW().';

COMMENT ON COLUMN drugdb.drug_interaction.updated_at IS
'Timestamp of the last update. Not auto-updated by trigger — update manually if '
'modifying severity or mechanism after LLM enrichment.';
