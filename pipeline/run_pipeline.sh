#!/bin/bash
# Master pipeline orchestration script for CDSS DrugDB
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/../logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_DIR/pipeline_$TIMESTAMP.log"; }

run_stage() {
  local stage_dir="$1"
  local stage_name="$2"
  log "=== Starting $stage_name ==="
  cd "$SCRIPT_DIR/$stage_dir"
  if [ -f "run.py" ]; then
    python3 run.py 2>&1 | tee -a "$LOG_DIR/pipeline_$TIMESTAMP.log"
  elif [ -f "run.sh" ]; then
    bash run.sh 2>&1 | tee -a "$LOG_DIR/pipeline_$TIMESTAMP.log"
  else
    log "WARNING: No run.py or run.sh found in $stage_dir"
  fi
  log "=== Completed $stage_name ==="
  cd "$SCRIPT_DIR"
}

log "Starting CDSS pipeline full run"

STAGE=${1:-"all"}

if [ "$STAGE" = "all" ] || [ "$STAGE" = "00" ]; then
  run_stage "00_setup" "Stage 00: Database Setup"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "01" ]; then
  run_stage "01_drug_table" "Stage 01: Drug Table Population"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "02" ]; then
  run_stage "02_ingredient_nodes" "Stage 02: Ingredient Nodes"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "03" ]; then
  run_stage "03_drug_ingredient_mapping" "Stage 03: Drug-Ingredient Mapping"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "04" ]; then
  run_stage "04_drug_interactions" "Stage 04: Drug Interactions"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "05" ]; then
  run_stage "05_drug_class" "Stage 05: Drug Class"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "06" ]; then
  run_stage "06_dosing_regimen" "Stage 06: Dosing Regimen"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "07" ]; then
  run_stage "07_indications" "Stage 07: Indications (Phase 3)"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "08" ]; then
  run_stage "08_clinical_sections" "Stage 08: Clinical Sections"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "09" ]; then
  run_stage "09_label_table" "Stage 09: Label Table"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "10" ]; then
  run_stage "10_vector_embeddings" "Stage 10: Vector Embeddings"
fi
if [ "$STAGE" = "all" ] || [ "$STAGE" = "11" ]; then
  run_stage "11_indian_brands" "Stage 11: Indian Brands"
fi

log "Pipeline run complete. Log: $LOG_DIR/pipeline_$TIMESTAMP.log"
