-- create_audit_table.sql
-- Run once before the first pipeline execution.
-- Connect as postgres to the postgres database.

CREATE TABLE IF NOT EXISTS drugdb.rxcui_resolution_audit (
    id                          SERIAL PRIMARY KEY,
    indian_brand_ingredient_id  INTEGER NOT NULL
                                    REFERENCES drugdb.indian_brand_ingredient(id),
    indian_brand_id             INTEGER NOT NULL,
    ingredient_name_norm        TEXT NOT NULL,
    ingredient_name_stripped    TEXT,           -- NULL for step1/step2
    synonym_used                TEXT,           -- NULL for step1
    matched_str                 TEXT NOT NULL,
    rxcui                       TEXT NOT NULL,
    tty_matched                 TEXT NOT NULL,  -- 'IN' or 'PIN'
    resolution_step             INTEGER NOT NULL CHECK (resolution_step IN (1, 2, 3)),
    match_confidence            TEXT NOT NULL,  -- 'exact','synonym','salt_strip_exact','salt_strip_synonym'
    resolved_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pipeline_run_id             TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_run_id
    ON drugdb.rxcui_resolution_audit(pipeline_run_id);

CREATE INDEX IF NOT EXISTS idx_audit_step
    ON drugdb.rxcui_resolution_audit(resolution_step);

CREATE INDEX IF NOT EXISTS idx_audit_ibi_id
    ON drugdb.rxcui_resolution_audit(indian_brand_ingredient_id);
