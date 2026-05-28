#!/usr/bin/env bash
# =============================================================================
# insert_drug.sh — Insert / update a drug record through the full pipeline
# Run from drug_pipeline/: bash insert_drug.sh
# =============================================================================

set -euo pipefail

LOG_FILE="logs/insert_drug_$(date '+%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1

PGHOST="${DB_HOST:-localhost}"
PGPORT="5432"
PGUSER="postgres"
PGDATABASE="postgres"
export PGPASSWORD="${DB_PASSWORD}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "================================================================="
log "  Drug Pipeline — Insert / Update Run"
log "================================================================="

# ── Phase 1: Schema Setup (idempotent — safe to re-run) ───────────────────
log ""
log "PHASE 1: Schema Setup"
log "-----------------------------------------------------------------"

log "1.1  Creating drug tables if not exist..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f sql/schema/create_drug_table.sql
log "     → drugdb.drug, drug_identifier, drug_synonym_formulation"

log "1.2  Applying enrichment column migrations..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f sql/migrations/alter_drug_table_new_columns.sql
log "     → 9 enrichment columns added (idempotent)"

log "1.3  Applying RxNorm column migrations..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f sql/migrations/add_rxnorm_columns.sql
log "     → rxcui, rxnorm_generic_formulation columns added"

log "1.4  Applying drug class column migrations..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f sql/migrations/drug_class_alter.sql
log "     → pharmacologic_class, therapeutic_class, mechanism_class columns added"

# ── Phase 2: Core Population ───────────────────────────────────────────────
log ""
log "PHASE 2: Core Population"
log "-----------------------------------------------------------------"

log "2.1  Populating drugdb.drug from DrugMasterLinkage..."
python scripts/populate_drug_table.py --password "$PGPASSWORD"
log "     → drugdb.drug populated"

log "2.2  Updating RxNorm columns..."
python scripts/update_drug_rxnorm_columns.py --password "$PGPASSWORD"
log "     → rxcui, rxnorm_generic_formulation updated"

log "2.3  Updating enrichment columns..."
python scripts/update_drug_new_columns.py --password "$PGPASSWORD"
log "     → enrichment columns updated"

# ── Verification ───────────────────────────────────────────────────────────
log ""
log "VERIFICATION"
log "-----------------------------------------------------------------"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" << 'SQL'
\echo '--- drugdb.drug row count ---'
SELECT COUNT(*) AS total_drugs FROM drugdb.drug;

\echo '--- RxCUI coverage ---'
SELECT
    COUNT(*) AS total,
    COUNT(rxcui) AS has_rxcui,
    COUNT(*) - COUNT(rxcui) AS missing_rxcui
FROM drugdb.drug;

\echo '--- Drug class coverage ---'
SELECT
    COUNT(*) FILTER (WHERE pharmacologic_class IS NOT NULL) AS has_pharm_class,
    COUNT(*) FILTER (WHERE therapeutic_class IS NOT NULL)   AS has_therapeutic_class
FROM drugdb.drug;
SQL

log ""
log "================================================================="
log "  Pipeline complete. Log written to $LOG_FILE"
log "================================================================="
