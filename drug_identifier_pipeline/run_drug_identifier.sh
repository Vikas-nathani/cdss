#!/usr/bin/env bash
# =============================================================================
# run_drug_identifier.sh — Populate drugdb.drug_identifier
# Run from drug_identifier_pipeline/: bash run_drug_identifier.sh
# =============================================================================
#
# Purpose:
#   Streams DrugMasterLinkage JOIN drugdb.drug and extracts all external
#   identifiers (rxcui, ndc_product, ndc_package, unii, upc, application_number,
#   spl_id, spl_set_id, drugbank) → inserts 578,635 rows.
#
# Prerequisites:
#   drug_pipeline/insert_drug.sh must have run (drugdb.drug populated)
#
# Resume-safe: ON CONFLICT (formulation_id, id_type, id_value) DO NOTHING
# =============================================================================

set -euo pipefail

LOG_FILE="logs/drug_identifier_$(date '+%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1

export PGPASSWORD="${DB_PASSWORD}"
PGHOST="${DB_HOST:-localhost}"; PGPORT="5432"; PGUSER="postgres"; PGDATABASE="postgres"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "================================================================="
log "  Drug Identifier Pipeline"
log "================================================================="

log ""
log "STEP 1: Schema setup (idempotent)..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f sql/drug_identifier_schema.sql
log "        → drugdb.drug_identifier table and indexes ready"

log ""
log "STEP 2: Populating drugdb.drug_identifier..."
python scripts/populate_drug_identifier.py --password "$PGPASSWORD"
log "        → drug_identifier populated"

log ""
log "VERIFICATION"
log "-----------------------------------------------------------------"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" << 'SQL'
\echo '--- Identifier breakdown by type ---'
SELECT id_type, COUNT(*) AS count
FROM drugdb.drug_identifier
GROUP BY id_type
ORDER BY count DESC;

\echo '--- Total rows ---'
SELECT COUNT(*) AS total FROM drugdb.drug_identifier;
SQL

log ""
log "================================================================="
log "  Pipeline complete. Log: $LOG_FILE"
log "================================================================="
