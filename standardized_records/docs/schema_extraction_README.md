# scripts/schema_extraction/

## Purpose

These scripts inspect raw source data stored in `DrugSourceMaster.clean_record` and extract a master schema template — a JSON object whose keys mirror every field path seen across all records, with all leaf values set to `null`. The output files are the inputs for the normalization scripts. Run these once per source, after the `DrugSourceMaster` table has been populated with raw data, to capture the full field inventory before normalizing.

## Scripts

### extract_drugbank_schema.py
- **What it does:** Two-phase schema extractor for DrugBank records. Phase 1 (default) fetches 2 sample rows, prints a preview of the extracted schema, and prompts for confirmation. Phase 2 (`--full`) streams all DrugBank rows through a server-side cursor, merges every record's structure into a running schema template, and writes the schema, stats, and a human-readable field catalog to disk.
- **Reads from:** `DrugSourceMaster.clean_record` where `source = 'drugbank'` (PostgreSQL)
- **Writes to:** `data/master_schema_drugbank.json`, `data/schema_stats_drugbank.json`, `data/drugbank_field_catalog.txt`, `data/drugbank_extraction_errors.log` (on parse errors)
- **When to run:** Before `normalize_drugbank.py`. Run Phase 1 first to verify, then Phase 2 for the full schema.
- **Usage:**
  ```bash
  # Phase 1 — preview (2 records)
  python scripts/schema_extraction/extract_drugbank_schema.py

  # Phase 2 — full extraction
  python scripts/schema_extraction/extract_drugbank_schema.py --full
  ```

### extract_schema_dailymed.py
- **What it does:** Two-phase schema extractor for DailyMed records. Phase 1 tests with 2 records and prints a schema preview. Phase 2 (`--phase2`) streams all ~51,731 DailyMed rows via a server-side cursor (batch size 500), merges them into a master schema, and saves the schema JSON and stats file.
- **Reads from:** `DrugSourceMaster.clean_record` where `source = 'dailymed'` (PostgreSQL)
- **Writes to:** `master_schema_dailymed.json`, `schema_stats_dailymed.json` (written to the working directory)
- **When to run:** Before `normalize_dailymed.py`. Run Phase 1 to confirm DB connectivity and record shape, then Phase 2 for the full schema.
- **Usage:**
  ```bash
  # Phase 1 — preview
  python scripts/schema_extraction/extract_schema_dailymed.py

  # Phase 2 — full extraction
  python scripts/schema_extraction/extract_schema_dailymed.py --phase2
  ```

### extract_schema_openfda.py
- **What it does:** Combined Phase 1 analysis and field-mapping design for all four sources (OpenFDA, DailyMed, RxNorm, DrugBank). Reads up to 10 sample `clean_record` rows per source, applies the full field-mapping rules (`OPENFDA_FLAT_MAP`, `DAILYMED_SECTION_TO_CAT`), and reports unmapped field coverage across 500-record scans. Writes per-source field-mapping documents and a transformation-samples file. (Note: despite the filename this script covers all four sources, not just OpenFDA.)
- **Reads from:** `DrugSourceMaster.clean_record` (all sources, PostgreSQL); `data/normalized_schema.json`; `data/master_schema_dailymed_normalized.json`
- **Writes to:** `field_mapping_openfda.json`, `field_mapping_dailymed.json`, `transformation_samples.json`, `phase1_analysis_report.txt` (written to the working directory)
- **When to run:** After both OpenFDA and DailyMed normalized schemas exist. Used to confirm mapping completeness before the full `standardize_records.py` Phase 2 run.
- **Usage:**
  ```bash
  python scripts/schema_extraction/extract_schema_openfda.py
  ```

## Dependencies

**Python packages:** `psycopg2-binary`

**Database tables that must exist:**
- `DrugSourceMaster` with columns `clean_record` (JSONB), `source` (text)

**Local data files required (for extract_schema_openfda.py only):**
- `data/normalized_schema.json`
- `data/master_schema_dailymed_normalized.json`

**Hardcoded DB connection:** host `178.236.185.230`, port `5432`, database `postgres`. Override by editing `DB_CONFIG` at the top of each file, or set `PG_*` environment variables where supported.
