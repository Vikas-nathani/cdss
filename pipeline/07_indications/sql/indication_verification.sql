-- ============================================================
-- Phase 3 — Post-Extraction Verification Queries
-- Run these AFTER the extraction script completes
-- and AFTER stopping the RunPod pod
-- ============================================================

-- 1. Overall count
SELECT
    COUNT(DISTINCT formulation_id) AS drugs_with_indications,
    COUNT(*) AS total_indication_rows,
    ROUND(COUNT(*)::numeric /
          NULLIF(COUNT(DISTINCT formulation_id), 0), 1)
        AS avg_indications_per_drug
FROM drugdb.drug_indication;

-- 2. Source distribution
SELECT
    source,
    COUNT(DISTINCT formulation_id) AS drugs,
    COUNT(*) AS indication_rows
FROM drugdb.drug_indication
GROUP BY source
ORDER BY drugs DESC;

-- 3. How many drugs were processed vs skipped
SELECT
    status,
    COUNT(*) AS drug_count,
    SUM(rows_inserted) AS total_rows_inserted
FROM drugdb.indication_extraction_log
GROUP BY status;

-- 4. ICD10 / SNOMED coverage
SELECT
    COUNT(*) AS total_rows,
    COUNT(icd10) AS has_icd10,
    COUNT(snomed) AS has_snomed,
    COUNT(mesh) AS has_mesh,
    ROUND(COUNT(icd10)::numeric / COUNT(*) * 100, 1)
        AS icd10_pct,
    ROUND(COUNT(snomed)::numeric / COUNT(*) * 100, 1)
        AS snomed_pct
FROM drugdb.drug_indication;

-- 5. Line of therapy distribution
SELECT
    line_of_therapy,
    COUNT(*) AS count,
    ROUND(COUNT(*)::numeric /
          SUM(COUNT(*)) OVER () * 100, 1) AS pct
FROM drugdb.drug_indication
GROUP BY line_of_therapy
ORDER BY count DESC;

-- 6. Top 20 most common indications across all drugs
SELECT
    term,
    COUNT(DISTINCT formulation_id) AS drug_count,
    MIN(icd10) AS sample_icd10
FROM drugdb.drug_indication
GROUP BY term
ORDER BY drug_count DESC
LIMIT 20;

-- 7. Drugs that failed extraction
SELECT
    log.formulation_id,
    log.error_message,
    d.generic_name
FROM drugdb.indication_extraction_log log
JOIN drugdb.drug d ON d.formulation_id = log.formulation_id
WHERE log.status = 'error'
ORDER BY log.processed_at DESC
LIMIT 20;

-- 8. Drugs with NO indications extracted despite having text
SELECT
    d.formulation_id,
    d.generic_name
FROM drugdb.drug d
LEFT JOIN drugdb.drug_indication di
    ON di.formulation_id = d.formulation_id
LEFT JOIN drugdb.indication_extraction_log log
    ON log.formulation_id = d.formulation_id
WHERE di.formulation_id IS NULL
  AND log.status = 'done'
LIMIT 20;
