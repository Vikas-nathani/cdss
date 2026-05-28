#!/usr/bin/env bash
# =============================================================================
# run_drug_synonym.sh — Populate drugdb.drug_synonym_formulation
# Run from drug_synonym_pipeline/: bash run_drug_synonym.sh
# =============================================================================
#
# Purpose:
#   Loads rxcui → formulation_id map from drugdb.drug, then extracts
#   synonyms[] from each rxnorm entry in DrugMasterLinkage and inserts
#   one row per formulation into drugdb.drug_synonym_formulation (~66K rows).
#
# Prerequisites (must complete before running this):
#   1. drug_pipeline/insert_drug.sh must have run
#      → drugdb.drug populated (~88,983 rows)
#      → drugdb.drug.rxcui filled (100% coverage required)
#
# Resume-safe: uses ON CONFLICT DO NOTHING — safe to re-run.
# =============================================================================

set -euo pipefail

LOG_FILE="logs/synonym_population_$(date '+%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1

PGHOST="${DB_HOST:-localhost}"
PGPORT="5432"
PGUSER="postgres"
PGDATABASE="postgres"
export PGPASSWORD="${DB_PASSWORD}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "================================================================="
log "  Drug Synonym Formulation Pipeline"
log "================================================================="

# ── Prerequisite check ────────────────────────────────────────────────────
log ""
log "PREREQUISITE CHECK"
log "-----------------------------------------------------------------"
log "Checking drugdb.drug.rxcui coverage..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" << 'SQL'
SELECT
    COUNT(*)                                      AS total_drugs,
    COUNT(rxcui)                                  AS has_rxcui,
    COUNT(*) - COUNT(rxcui)                       AS missing_rxcui,
    ROUND(COUNT(rxcui)::numeric / COUNT(*) * 100, 2) AS pct_covered
FROM drugdb.drug;
SQL

# ── Populate drug_synonym_formulation ─────────────────────────────────────
log ""
log "STEP 1: Populating drugdb.drug_synonym_formulation..."
log "        Extracts synonyms[] from DrugMasterLinkage rxnorm entries"
python scripts/populate_drug_synonym_formulation.py --password "$PGPASSWORD"
log "        → drug_synonym_formulation populated"

# ── Verification ──────────────────────────────────────────────────────────
log ""
log "VERIFICATION"
log "-----------------------------------------------------------------"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" << 'SQL'
\echo '--- drug_synonym_formulation coverage ---'
SELECT
    COUNT(DISTINCT formulation_id)  AS formulations_with_synonyms,
    (SELECT COUNT(*) FROM drugdb.drug) AS total_drugs,
    ROUND(
        COUNT(DISTINCT formulation_id)::numeric /
        (SELECT COUNT(*) FROM drugdb.drug) * 100, 2
    ) AS pct_coverage
FROM drugdb.drug_synonym_formulation;
SQL

log ""
log "================================================================="
log "  Pipeline complete. Log written to $LOG_FILE"
log "================================================================="
