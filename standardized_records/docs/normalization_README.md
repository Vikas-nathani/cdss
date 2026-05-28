# scripts/normalization/

## Purpose

These scripts clean, reshape, and semantically reorganize raw source data (DailyMed, DrugBank, OpenFDA) before it is stored or used downstream. They operate on JSON schema files or the `DrugSourceMaster` table's `clean_record` column and produce normalized output — either new JSON files in `data/` or populated `standardized_records` / `standardized_data` JSONB columns. Run these after raw extraction is complete and before any pipeline stage that depends on a uniform schema.

## Scripts

### normalize_dailymed.py
- **What it does:** Applies "Option A" semantic normalization to `data/master_schema_dailymed.json` — drops the `label_sections` wrapper and distributes its children directly under eight semantic category keys (`safety`, `clinical`, `drug_interactions`, etc.). Verifies zero field loss, then writes the normalized schema and field-mapping files.
- **Reads from:** `data/master_schema_dailymed.json` (local file)
- **Writes to:** `data/master_schema_dailymed_normalized.json`, `data/dailymed_field_mapping.json`, `data/normalization_stats.json`, `data/verification_report.txt`
- **When to run:** Once, after `extract_schema_dailymed.py` has produced `master_schema_dailymed.json` and before `standardize_records.py` needs the DailyMed template.
- **Usage:**
  ```bash
  python scripts/normalization/normalize_dailymed.py
  ```

### normalize_drugbank.py
- **What it does:** Reorganizes the flat `data/master_schema_drugbank.json` into four semantic categories (`drug_info`, `clinical`, `drug_interactions`, `chemistry`). Logs a complete field-path mapping for all 19 original fields and verifies zero loss before writing output files.
- **Reads from:** `data/master_schema_drugbank.json` (local file)
- **Writes to:** `data/master_schema_drugbank_normalized.json`, `data/drugbank_field_mapping.json`, `data/normalization_stats_drugbank.json`, `data/drugbank_normalization_report.txt`
- **When to run:** Once, after `extract_drugbank_schema.py` has produced `master_schema_drugbank.json`.
- **Usage:**
  ```bash
  python scripts/normalization/normalize_drugbank.py
  ```

### normalize_openfda.py
- **What it does:** Phase 1 analysis and field-mapping design for all four sources (OpenFDA, DailyMed, RxNorm, DrugBank). Connects to `DrugSourceMaster`, fetches 10 sample `clean_record` rows per source, applies the full `OPENFDA_FLAT_MAP` and `DAILYMED_SECTION_TO_CAT` transformation rules, and reports any unmapped fields across 500-record scans. Writes per-source field-mapping JSON files and a transformation-samples file.
- **Reads from:** `DrugSourceMaster.clean_record` (PostgreSQL, `source IN ('openfda','dailymed','rxnorm','drugbank')`), `data/normalized_schema.json`, `data/master_schema_dailymed_normalized.json`
- **Writes to:** `field_mapping_openfda.json`, `field_mapping_dailymed.json`, `transformation_samples.json`, `phase1_analysis_report.txt` (all written to the working directory)
- **When to run:** After both normalization schema files exist and before committing to the full `standardize_records.py` Phase 2 run. Used to confirm mapping coverage.
- **Usage:**
  ```bash
  python scripts/normalization/normalize_openfda.py
  ```

### standardize_records.py
- **What it does:** Populates the `DrugSourceMaster.standardized_records` JSONB column for all rows. Phase 1 ensures the column exists, samples 2 records per source, and saves `sample_transformations.json`. Phase 2 (triggered by `--phase2`) streams all rows in batches of 1,000, deep-merges each `record` column value onto the appropriate source template (OpenFDA or DailyMed), and copies RxNorm/DrugBank records verbatim.
- **Reads from:** `DrugSourceMaster.record` (all sources); `data/normalized_schema.json` (OpenFDA template); `data/master_schema_dailymed_normalized.json` (DailyMed template)
- **Writes to:** `DrugSourceMaster.standardized_records` (JSONB column, created if absent); `sample_transformations.json`; `standardization_stats.json`; `standardization_errors.log`
- **When to run:** After normalization templates are in place (`normalize_dailymed.py`, `normalize_openfda.py` have been run). Phase 1 always runs first; pass `--phase2` to populate all rows.
- **Usage:**
  ```bash
  # Phase 1 only (preview, no full write)
  python scripts/normalization/standardize_records.py

  # Phase 1 + Phase 2 (full population)
  python scripts/normalization/standardize_records.py --phase2
  ```

### transform_to_unified.py
- **What it does:** Transforms a consolidated raw JSON object (containing `openfda`, `dailymed`, `rxnorm`, and `drugbank` sub-keys) into a single unified record matching `cdss_unified_schema.json`. Handles cross-source merging (DailyMed wins on structure, OpenFDA on completeness), normalizes RxNorm entries into clinical formulations, folds DrugBank interaction data, preserves label tables with semantic tags, and seeds `structured_facts` stubs for a downstream NLP/LLM enrichment pass.
- **Reads from:** A consolidated JSON file (default: `/mnt/user-data/uploads/ConsolidatedJSonFromFDSDrugBankDailyMedRXnorm.json`) passed as `argv[1]`
- **Writes to:** `unified_record.json` (default) or the path given as `argv[2]`
- **When to run:** During ingest, after all four source records for a drug have been linked. Can also be imported as a library module (`from transform_to_unified import transform`).
- **Usage:**
  ```bash
  python scripts/normalization/transform_to_unified.py <input.json> [output.json]
  ```

## Dependencies

**Python packages:** `psycopg2-binary`, `tqdm`

**Environment variables (optional overrides for standardize_records.py):** `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DB`

**Database tables that must exist:**
- `DrugSourceMaster` with columns `record` (JSONB), `clean_record` (JSONB), `source` (text), `id` (UUID PK)

**Local data files required before running:**
- `data/normalized_schema.json` — OpenFDA normalized schema template
- `data/master_schema_dailymed_normalized.json` — produced by `normalize_dailymed.py`
- `data/master_schema_drugbank.json` — produced by `extract_drugbank_schema.py` (required by `normalize_drugbank.py`)
- `data/master_schema_dailymed.json` — produced by `extract_schema_dailymed.py` (required by `normalize_dailymed.py`)
