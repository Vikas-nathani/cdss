-- Expand resolution_step check constraint to allow steps 4 and 5.
ALTER TABLE drugdb.rxcui_resolution_audit
    DROP CONSTRAINT IF EXISTS rxcui_resolution_audit_resolution_step_check;

ALTER TABLE drugdb.rxcui_resolution_audit
    ADD CONSTRAINT rxcui_resolution_audit_resolution_step_check
    CHECK (resolution_step IN (1, 2, 3, 4, 5));
