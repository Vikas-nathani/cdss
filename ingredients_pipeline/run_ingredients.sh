#!/usr/bin/env bash
# =============================================================================
# run_ingredients.sh — Populate drugdb.ingredients
# Run from ingredients_pipeline/: bash run_ingredients.sh
# =============================================================================
#
# Purpose:
#   1. Creates drugdb schema + tables via ingredient_schema.sql
#   2. Populates drugdb.ingredients from DrugSourceMaster (19,842 rows via drugdb_migration.sql)
#   3. Backfills rxcui on ingredients from DrugMasterLinkage
#
# Resume-safe: all inserts use ON CONFLICT DO NOTHING.
# =============================================================================

set -euo pipefail

LOG_FILE="logs/ingredients_$(date '+%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1

export PGPASSWORD="${DB_PASSWORD}"
PGHOST="${DB_HOST:-localhost}"; PGPORT="5432"; PGUSER="postgres"; PGDATABASE="postgres"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "================================================================="
log "  Ingredients Pipeline"
log "================================================================="

log ""
log "STEP 1: Schema setup — drugdb schema + tables + indexes..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f sql/ingredient_schema.sql
log "        → drugdb.ingredients, ingredient_synonyms, ingredient_interactions created"

log ""
log "STEP 2: Populating from DrugSourceMaster (DrugBank rows)..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f sql/drugdb_migration.sql
log "        → drugdb.ingredients populated (~19,842 rows)"

log ""
log "STEP 3: Backfilling rxcui from DrugMasterLinkage..."
python scripts/update_ingredient_rxcui.py --password "$PGPASSWORD"
log "        → rxcui backfilled (~2,137 rows updated)"

log ""
log "VERIFICATION"
log "-----------------------------------------------------------------"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" << 'SQL'
\echo '--- Row counts ---'
SELECT 'ingredients'            AS tbl, COUNT(*) FROM drugdb.ingredients
UNION ALL
SELECT 'ingredient_synonyms',           COUNT(*) FROM drugdb.ingredient_synonyms
UNION ALL
SELECT 'ingredient_interactions',       COUNT(*) FROM drugdb.ingredient_interactions;

\echo '--- RxCUI coverage ---'
SELECT
    COUNT(*)            AS total,
    COUNT(rxcui)        AS has_rxcui,
    ROUND(COUNT(rxcui)::numeric / COUNT(*) * 100, 2) AS pct
FROM drugdb.ingredients;

\echo '--- DrugBank ID coverage ---'
SELECT
    COUNT(*)               AS total,
    COUNT(drugbank_id)     AS has_drugbank_id,
    ROUND(COUNT(drugbank_id)::numeric / COUNT(*) * 100, 2) AS pct
FROM drugdb.ingredients;
SQL

log ""
log "================================================================="
log "  Pipeline complete. Log: $LOG_FILE"
log "================================================================="
