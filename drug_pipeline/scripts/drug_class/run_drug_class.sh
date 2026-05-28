#!/usr/bin/env bash
# =============================================================================
# run_drug_class.sh — Drug class extraction using RunPod A100 (Qwen2.5-7B)
# Run from drug_pipeline/scripts/drug_class/: bash run_drug_class.sh
# =============================================================================
#
# What this does:
#   1. Runs run.py  — async LLM extraction via RunPod, updates drugdb.drug
#                     with pharmacologic_class, therapeutic_class, mechanism_class
#   2. Runs drug_class_test.py — smoke test to verify a sample has class data
#
# Checkpoint: run.py is checkpointed — safe to stop and resume.
# Logs written to: drug_pipeline/logs/drug_class/
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/../../logs/drug_class"
LOG_FILE="$LOG_DIR/drug_class_run_$(date '+%Y%m%d_%H%M%S').log"

exec > >(tee -a "$LOG_FILE") 2>&1

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "================================================================="
log "  Drug Class Extraction — RunPod A100 / Qwen2.5-7B-Instruct"
log "================================================================="

# ── Step 1: Extract drug classes via LLM ──────────────────────────────────
log ""
log "STEP 1: Running drug class extraction (checkpointed — safe to resume)..."
log "        Updates drugdb.drug: pharmacologic_class, therapeutic_class, mechanism_class"
python "$SCRIPT_DIR/run.py"
log "        → Extraction complete"

# ── Step 2: Smoke test ────────────────────────────────────────────────────
log ""
log "STEP 2: Running smoke test to verify class data..."
python "$SCRIPT_DIR/drug_class_test.py"
log "        → Smoke test passed"

log ""
log "================================================================="
log "  Drug class extraction complete. Log: $LOG_FILE"
log "================================================================="
