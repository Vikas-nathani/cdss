#!/usr/bin/env bash
# =============================================================================
# run_drug_ingredient_mapping.sh — Populate drugdb.drug_ingredient_mapping
# Run from drug_ingredient_mapping_pipeline/: bash run_drug_ingredient_mapping.sh
# =============================================================================
#
# Prerequisites:
#   1. drug_pipeline/insert_drug.sh must have run (drugdb.drug populated, rxcui filled)
#   2. ingredients_pipeline/run_ingredients.sh must have run (drugdb.ingredients populated)
#
# Resume-safe: ON CONFLICT DO NOTHING on all inserts.
# =============================================================================

set -euo pipefail

LOG_FILE="logs/ingredient_mapping_$(date '+%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1

export PGPASSWORD="${DB_PASSWORD}"
PGHOST="${DB_HOST:-localhost}"; PGPORT="5432"; PGUSER="postgres"; PGDATABASE="postgres"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "================================================================="
log "  Drug Ingredient Mapping Pipeline"
log "================================================================="

log ""
log "STEP 1: First-pass population..."
python scripts/run.py --password "$PGPASSWORD"
log "        → drugdb.drug_ingredient_mapping populated"

log ""
log "STEP 2: Second-pass (retry unmatched ingredients)..."
python scripts/run_pass2.py --password "$PGPASSWORD"
log "        → Second-pass complete"

log ""
log "VERIFICATION"
log "-----------------------------------------------------------------"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" << 'SQL'
SELECT
    COUNT(DISTINCT d.formulation_id)   AS total_formulations,
    COUNT(DISTINCT dim.formulation_id) AS mapped_formulations,
    ROUND(100.0 * COUNT(DISTINCT dim.formulation_id) /
          COUNT(DISTINCT d.formulation_id), 2) AS pct_coverage,
    COUNT(*)                           AS total_mapping_rows
FROM drugdb.drug d
LEFT JOIN drugdb.drug_ingredient_mapping dim ON dim.formulation_id = d.formulation_id;
SQL

log ""
log "================================================================="
log "  Pipeline complete. Log: $LOG_FILE"
log "================================================================="
