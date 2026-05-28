#!/usr/bin/env bash
# =============================================================================
# run_all.sh — Full drug data pipeline execution
# Run from standardized_records/: bash run_all.sh
# =============================================================================

set -euo pipefail

LOG_FILE="logs/run_all_$(date '+%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1

PGHOST="${DB_HOST:-localhost}"
PGPORT="5432"
PGUSER="postgres"
PGDATABASE="postgres"
export PGPASSWORD="${DB_PASSWORD}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "================================================================="
log "  Drug Data Pipeline — Full Run"
log "================================================================="

# ── Phase 1: Schema Extraction ─────────────────────────────────────────────
log ""
log "PHASE 1: Schema Extraction"
log "-----------------------------------------------------------------"

log "1.1  Extracting OpenFDA schema..."
python scripts/extract_schema_openfda.py
log "     → data/master_schema_openfda.json"

log "1.2  Extracting DailyMed schema..."
python scripts/extract_schema_dailymed.py
log "     → data/master_schema_dailymed.json"

log "1.3  Extracting DrugBank schema..."
python scripts/extract_drugbank_schema.py
log "     → data/master_schema_drugbank.json"

# ── Phase 2: Schema Normalization ──────────────────────────────────────────
log ""
log "PHASE 2: Schema Normalization"
log "-----------------------------------------------------------------"

log "2.1  Normalizing OpenFDA schema..."
python scripts/normalize_openfda.py
log "     → data/normalized_schema_openfda.json"

log "2.2  Normalizing DailyMed schema..."
python scripts/normalize_dailymed.py
log "     → data/normalized_schema_dailymed.json"

log "2.3  Normalizing DrugBank schema..."
python scripts/normalize_drugbank.py
log "     → data/normalized_schema_drugbank.json"

# ── Phase 3: Data Standardization ─────────────────────────────────────────
log ""
log "PHASE 3: Data Standardization (738,197 rows — expect 45-90 min)"
log "-----------------------------------------------------------------"

log "3.1  Populating standardized_records for all sources..."
python scripts/standardize_records.py
log "     → DrugSourceMaster.standardized_records (all sources)"

# ── Phase 4: Verify schema exists ─────────────────────────────────────────
log ""
log "PHASE 4: Target Schema Verification"
log "-----------------------------------------------------------------"
log "4.1  Checking drugdb schema tables..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -c "SELECT table_name FROM information_schema.tables WHERE table_schema='drugdb' ORDER BY table_name;"

# ── Phase 5: Database Population ──────────────────────────────────────────
log ""
log "PHASE 5: Populating drugdb tables (expect 10-15 min)"
log "-----------------------------------------------------------------"

log "5.1  Running drugdb_migration.sql..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f schemas/drugdb_migration.sql
log "     → drugdb.ingredients, ingredient_synonyms, ingredient_interactions"

# ── Final Verification ─────────────────────────────────────────────────────
log ""
log "VERIFICATION"
log "-----------------------------------------------------------------"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" << 'SQL'
\echo '--- Phase 3: standardized_records population ---'
SELECT source,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE standardized_records IS NOT NULL) AS populated
FROM public."DrugSourceMaster"
GROUP BY source ORDER BY source;

\echo '--- Phase 5: drugdb row counts ---'
SELECT 'ingredients'         AS tbl, COUNT(*) FROM drugdb.ingredients
UNION ALL
SELECT 'ingredient_synonyms',         COUNT(*) FROM drugdb.ingredient_synonyms
UNION ALL
SELECT 'ingredient_interactions',     COUNT(*) FROM drugdb.ingredient_interactions;
SQL

log ""
log "================================================================="
log "  Pipeline complete."
log "================================================================="
