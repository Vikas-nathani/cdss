# Codebase Pipeline Documentation

> **Generated:** 2026-05-05  
> **Database:** PostgreSQL @ 178.236.185.230 (database: `postgres`)  
> **Project Root:** `/home/nathanivikas890_gmail_com/cdss`

---

## Overview

This codebase is a **Clinical Decision Support System (CDSS)** drug data pipeline. It ingests 738,197 raw drug records from four sources (OpenFDA, DailyMed, DrugBank, RxNorm), normalizes them into a unified schema, and populates a set of PostgreSQL tables that power a FastAPI-based REST API. The API answers nine clinical queries (Q1–Q9) covering indications, drug interactions, dosage recommendations, population approvals, alternatives, and more.

The pipeline has five completed phases (schema extraction → normalization → standardization → schema creation → database population) and several pending phases (LLM-assisted extraction, RAG embedding, Neo4j graph, Indian brand loading, API endpoint implementation).

**Data sources:**
| Source | Records | Description |
|--------|---------|-------------|
| OpenFDA | 256,165 | FDA drug labels (structured JSON) |
| DailyMed | 51,731 | DailyMed product labels |
| DrugBank | 19,842 | DrugBank ingredient/interaction reference |
| RxNorm | 410,459 | RxNorm formulation concepts |
| **Total** | **738,197** | All stored in `public.DrugSourceMaster` |

---

## Table of Contents

1. [DrugSourceMaster](#table-drugsourcemaster) — Source input table
2. [drugdb.drug](#table-publicdrug) — Core drug formulation table
3. [drugdb.drug_synonym_formulation](#table-publicdrug_synonym_formulation) — Synonym arrays per formulation
4. [drugdb.drug_ingredient_mapping](#table-publicdrug_ingredient_mapping) — Drug–ingredient relationships
5. [drugdb.drug_identifier](#table-publicdrug_identifier) — Universal external ID resolver
6. [drugdb.ingredients](#table-drugdbingredients) — DrugBank ingredient reference
7. [drugdb.ingredient_synonyms](#table-drugdbingredient_synonyms) — Ingredient alternate names
8. [drugdb.ingredient_interactions](#table-drugdbingredient_interactions) — Pairwise interaction matrix
9. [drugdb.drug_interaction](#table-drugdbdrug_interaction) — Formulation-level interaction pairs
10. [Severity/Mechanism Enrichment Pipeline](#severity-enrichment-pipeline) — LLM enrichment of ingredient_interactions
11. [Pending Tables](#pending-tables-phase-2) — Structured facts, RAG, Indian brands
11. [Cross-Table Dependencies](#cross-table-dependencies)
12. [Full Pipeline Sequence (All Phases)](#full-pipeline-sequence-all-phases)
13. [Complete File Inventory](#complete-file-inventory)
14. [Infrastructure & Configuration](#infrastructure--configuration)
15. [FastAPI Application Layer](#fastapi-application-layer)

---

## Table: DrugSourceMaster

**Phase:** Pre-existing (Day-0 input)  
**Schema:** `public`  
**Rows:** 738,197  
**Purpose:** The master source-of-truth table. Each row holds one raw drug record from one of four sources. This table is never written to by this pipeline except for the `standardized_records` column added in Phase 3.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `source` | TEXT | `openfda`, `dailymed`, `drugbank`, or `rxnorm` |
| `sourceid` | TEXT | Source-native ID (e.g. DrugBank ID `DB00001`) |
| `clean_record` | JSONB | Pre-processed record (openfda, dailymed, drugbank) |
| `record` | JSONB | Raw record for rxnorm (no clean_record equivalent) |
| `standardized_records` | JSONB | Added by Phase 3 — normalized into semantic categories |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `schemas/verification_queries.sql` | Verify source row counts | `DrugSourceMaster` | Counts per source |
| 2 | `scripts/extract_schema_openfda.py` | Phase 1: extract all unique field paths from OpenFDA | `DrugSourceMaster WHERE source='openfda'` (256,165 rows) | `data/master_schema_openfda.json` |
| 3 | `scripts/extract_schema_dailymed.py` | Phase 1: extract all unique field paths from DailyMed | `DrugSourceMaster WHERE source='dailymed'` (51,731 rows) | `data/master_schema_dailymed.json` |
| 4 | `scripts/extract_drugbank_schema.py` | Phase 1: extract all unique field paths from DrugBank | `DrugSourceMaster WHERE source='drugbank'` (19,842 rows) | `data/master_schema_drugbank.json` |
| 5 | `scripts/normalize_openfda.py` | Phase 2: reorganize 163 flat OpenFDA fields into 13 semantic categories | `data/master_schema_openfda.json` | `data/normalized_schema.json` |
| 6 | `scripts/normalize_dailymed.py` | Phase 2: reorganize DailyMed fields into 10 semantic categories | `data/master_schema_dailymed.json` | `data/master_schema_dailymed_normalized.json` |
| 7 | `scripts/normalize_drugbank.py` | Phase 2: reorganize 11 DrugBank fields into 4 semantic categories | `data/master_schema_drugbank.json` | `data/master_schema_drugbank_normalized.json` |
| 8 | `scripts/populate_standardized.py` | Phase 3: overlay openfda+dailymed records onto normalized template | `clean_record` + normalized schema JSONs | `DrugSourceMaster.standardized_records` (307,896 rows) |
| 9 | `scripts/populate_remaining.py` | Phase 3: overlay drugbank+rxnorm records onto normalized template | `clean_record`/`record` + normalized schema JSONs | `DrugSourceMaster.standardized_records` (430,301 rows) |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `scripts/extract_schema_openfda.py` | Python | Discovers all unique OpenFDA field paths |
| `scripts/extract_schema_dailymed.py` | Python | Discovers all unique DailyMed field paths |
| `scripts/extract_drugbank_schema.py` | Python | Discovers all unique DrugBank field paths |
| `scripts/normalize_openfda.py` | Python | Reorganizes OpenFDA fields into semantic groups |
| `scripts/normalize_dailymed.py` | Python | Reorganizes DailyMed fields into semantic groups |
| `scripts/normalize_drugbank.py` | Python | Reorganizes DrugBank fields into semantic groups |
| `scripts/populate_standardized.py` | Python | Writes standardized_records for openfda + dailymed |
| `scripts/populate_remaining.py` | Python | Writes standardized_records for drugbank + rxnorm |
| `populate_label_table.py` | Python | Phase 6: extracts label tables from combined_clean_jsonb into drugdb.label_table (510,527 rows) |
| `scripts/verify_schema.py` | Python | Validates schema overlay logic (read-only, utility) |
| `scripts/compare_records.py` | Python | Compares records across sources for consistency |
| `scripts/standardize_records.py` | Python | Earlier draft of Phase 3 (superseded, kept for reference) |
| `data/master_schema_openfda.json` | JSON | 163 unique top-level OpenFDA field paths |
| `data/master_schema_dailymed.json` | JSON | DailyMed structural keys (4 keys) |
| `data/master_schema_drugbank.json` | JSON | 11 unique DrugBank field paths |
| `data/normalized_schema.json` | JSON | OpenFDA fields in 13 semantic categories |
| `data/master_schema_dailymed_normalized.json` | JSON | DailyMed fields in 10 semantic categories |
| `data/master_schema_drugbank_normalized.json` | JSON | DrugBank fields in 4 semantic categories |
| `data/field_mapping_openfda.json` | JSON | Raw OpenFDA field → normalized path mapping |
| `data/field_mapping_dailymed.json` | JSON | Raw DailyMed field → normalized path mapping |
| `data/drugbank_field_mapping.json` | JSON | Raw DrugBank field → normalized path mapping |
| `data/normalization_stats.json` | JSON | Stats from normalization run (before/after/categories) |
| `data/normalization_stats_drugbank.json` | JSON | DrugBank-specific normalization stats |
| `data/transformation_samples.json` | JSON | Before/after sample records (10 per source) |
| `data/comparison_detail.json` | JSON | Field-level comparison across sources |
| `data/drugbank_field_catalog.txt` | Text | Human-readable DrugBank field catalog |
| `data/drugbank_normalization_report.txt` | Text | DrugBank normalization run report |
| `data/sample_rows.json` | JSON | First 2 raw rows from last verify_schema.py run |
| `data/sample_master_schema.json` | JSON | Schema derived from verify_schema.py sample |
| `data/normalization_stats.json` | JSON | Normalization run statistics |
| `data/schema_stats_dailymed.json` | JSON | DailyMed field statistics |
| `data/schema_stats_drugbank.json` | JSON | DrugBank field statistics |
| `data/normalized_schema_dailymed.json` | JSON | Alternative DailyMed normalized schema variant |
| `data/normalized_schema_drugbank.json` | JSON | Alternative DrugBank normalized schema variant |
| `data/normalized_schema_openfda.json` | JSON | Alternative OpenFDA normalized schema variant |
| `docs/cdss_unified_schema.json` | JSON | Target unified schema (reference) |
| `data/samples/sample_raw_input.json` | JSON | Sample raw input for testing |
| `data/samples/unified_sample.json` | JSON | Sample unified output |
| `data/samples/unified_sample_enriched.json` | JSON | Sample enriched unified output |

### Notes for New Contributors

To re-run the standardization for all 738,197 records (e.g., after schema changes):
```bash
cd /home/nathanivikas890_gmail_com/cdss

# Phase 1: Re-extract schemas from source
python scripts/extract_schema_openfda.py
python scripts/extract_schema_dailymed.py
python scripts/extract_drugbank_schema.py

# Phase 2: Re-normalize
python scripts/normalize_openfda.py
python scripts/normalize_dailymed.py
python scripts/normalize_drugbank.py

# Phase 3: Re-populate standardized_records
python scripts/populate_standardized.py    # openfda + dailymed
python scripts/populate_remaining.py       # drugbank + rxnorm
```

Expected timing: Phase 1 ~5 min, Phase 2 ~2 min, Phase 3 ~45–90 min.

---

## Table: drugdb.drug

**Phase:** Phase 1 (Day-2 initial creation) + Phase 5 (enrichment columns added)  
**Schema:** `public`  
**Rows:** 88,983  
**Purpose:** One row per unique RxNorm formulation. This is the central lookup table used by all other tables and the API. Every row represents a specific drug-dose-form combination (e.g., "metformin 500 MG Extended Release Oral Tablet").

### Key Columns (All Columns)

| Column | Type | Description |
|--------|------|-------------|
| `formulation_id` | UUID PK | Deterministic UUID5 from (master_linkage_id, generic_formulation, dosage_form) |
| `generic_name` | TEXT | Drug's generic name from OpenFDA |
| `generic_formulation` | TEXT | Cleaned name: dose stripped of form suffix (e.g. "metformin 500 MG") |
| `dosage_forms` | TEXT | The specific dosage form (e.g. "Oral Tablet") |
| `master_linkage_id` | UUID | FK back to `DrugMasterLinkage.master_linkage_id` |
| `rxnorm_generic_formulation` | TEXT | Full RxNorm name (added by `update_drug_rxnorm_columns.py`) |
| `rxcui` | VARCHAR(50) | RxNorm concept unique identifier (added by `update_drug_rxnorm_columns.py`) |
| `product_type` | TEXT | e.g. "HUMAN PRESCRIPTION DRUG", "HUMAN OTC DRUG" (added by `update_drug_new_columns.py`) |
| `routes` | TEXT[] | Routes of administration (added by `update_drug_new_columns.py`) |
| `mechanism_of_action` | TEXT | Truncated to 5000 chars (added by `update_drug_new_columns.py`) |
| `record_version` | TEXT | Label version from DailyMed (added by `update_drug_new_columns.py`) |
| `last_ingested_at` | TIMESTAMPTZ | Timestamp when enrichment ran |
| `has_openfda` | BOOLEAN | Whether record has OpenFDA data |
| `has_dailymed` | BOOLEAN | Whether record has DailyMed data |
| `has_rxnorm` | BOOLEAN | Whether record has RxNorm data |
| `has_drugbank` | BOOLEAN | Whether record has DrugBank data |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `schemas/create_drug_table.sql` | DDL: creates base drug table if needed (helper, optional) | None | `drugdb.drug` (DDL) |
| 2 | `scripts/populate_drug_table.py` | Core: inserts 88,983 rows — one per rxnorm entry in DrugMasterLinkage | `DrugMasterLinkage.combined_clean_jsonb` (rxnorm[] entries) | `drugdb.drug` (88,983 rows) |
| 3 | `scripts/alter_drug_table_new_columns.sql` | DDL: adds 9 enrichment columns (product_type, routes, etc.) | `drugdb.drug` (existing table) | 9 new columns added |
| 4 | `scripts/add_rxnorm_columns.sql` | DDL: adds rxnorm_generic_formulation and rxcui columns | `drugdb.drug` (existing table) | 2 columns added |
| 5 | `scripts/update_drug_rxnorm_columns.py` | Fills rxcui + rxnorm_generic_formulation by matching formulation_id UUID seed | `DrugMasterLinkage.combined_clean_jsonb` | `drugdb.drug.rxcui` (88,983 filled), `drugdb.drug.rxnorm_generic_formulation` |
| 6 | `scripts/update_drug_new_columns.py` | Fills 9 enrichment columns from combined_clean_jsonb | `DrugMasterLinkage JOIN drugdb.drug` (88,983 rows) | `drugdb.drug` product_type, routes, mechanism_of_action, has_* columns |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `schemas/create_drug_table.sql` | SQL | DDL helper for creating the base drug table |
| `schemas/alter_drug_table_new_columns.sql` → actually at `scripts/alter_drug_table_new_columns.sql` | SQL | Adds 9 enrichment columns |
| `scripts/add_rxnorm_columns.sql` | SQL | Adds rxcui and rxnorm_generic_formulation columns |
| `scripts/populate_drug_table.py` | Python | Inserts all 88,983 drug rows from DrugMasterLinkage rxnorm entries |
| `scripts/update_drug_rxnorm_columns.py` | Python | Fills rxcui and rxnorm_generic_formulation using UUID5 seed matching |
| `scripts/update_drug_new_columns.py` | Python | Fills 9 enrichment columns from combined JSONB |
| `scripts/fix_rxnorm_uncleaned_rows.py` | Python | Fixes rows where rxnorm column wasn't properly cleaned |
| `verify_dosage_cleanup.py` | Python | Verifies that dosage form suffixes were properly stripped |
| `build_dosage_mappings.py` | Python | Utility that built the dosage form suffix map (now baked into populate_drug_table.py) |
| `unique_dosage_forms_in_drug_table.csv` | CSV | Unique dosage_forms values seen in drug table |
| `unique_specific_dosage_forms.csv` | CSV | Unique specific_dosage_form values from DrugMasterLinkage |
| `dosage_form_mappings.csv` | CSV | Mapping of EU dosage form codes → RxNorm suffix phrases |
| `dosage_form_mappings.json` | JSON | Same mapping in JSON format |
| `dosage_form_mismatches.csv` | CSV | Dosage forms that didn't match any known suffix |
| `dosage_form_regex_patterns.json` | JSON | Compiled regex patterns for suffix stripping |
| `generic_formulation_ending_patterns.csv` | CSV | Patterns found at end of generic_formulation strings |
| `comparison_report.txt` | Text | Report comparing old vs new dosage form values |
| `logs/populate_drug_table.log` | Log | Tracks populate_drug_table.py run (2026-05-02, ~5 attempts before fix) |
| `logs/rxnorm_update.log` | Log | Tracks update_drug_rxnorm_columns.py run |
| `logs/rxnorm_dryrun.log` | Log | Dry-run of rxnorm column update |
| `logs/rxnorm_fix.log` | Log | Log from fix_rxnorm_uncleaned_rows.py run |
| `logs/rxnorm_fix_dryrun.log` | Log | Dry-run of rxnorm fix |
| `logs/drug_enrichment.log` | Log | Tracks update_drug_new_columns.py run (2026-05-05, 88,983 rows in 738s) |
| `logs/drug_enrichment_dryrun.log` | Log | Dry-run of drug enrichment |
| `reports/rxnorm_columns_population_report.md` | Markdown | Report on rxnorm column population status |
| `phase1_analysis_report.txt` | Text | Analysis report from Phase 1 design work |
| `verification_report.txt` | Text | Verification results from schema checks |
| `tablecreation.md` | Markdown | Brief notes on drug table creation commands |

### Notes for New Contributors

To add a new record to `drugdb.drug`:
1. The record must first exist in `public.DrugMasterLinkage.combined_clean_jsonb` with a `rxnorm[]` array.
2. Run `python3 scripts/populate_drug_table.py --password Admin@123 --batch-size 1000 --log-file logs/drug_populate.log` — it uses ON CONFLICT DO NOTHING, so re-runs are safe.
3. Then run `python3 scripts/update_drug_rxnorm_columns.py --password Admin@123` to fill rxcui.
4. Then run `python3 scripts/update_drug_new_columns.py --password Admin@123` to fill enrichment columns.

Always do a dry-run first: add `--dry-run --verbose` flags to both scripts.

---

## Table: drugdb.drug_synonym_formulation

**Phase:** Phase 1 (populated after drug table + rxcui fill)  
**Schema:** `public`  
**Rows:** ~66,000 (74.34% of 88,983 formulations have synonyms)  
**Purpose:** Stores alternate brand names / synonym arrays for each formulation. Used by the entity resolver to fuzzy-match user-supplied drug names to a `formulation_id`.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment |
| `formulation_id` | UUID FK → drug | Links to the drug formulation |
| `synonyms` | TEXT[] | Array of synonym names from RxNorm |

### Indexes

| Index | Type | Purpose |
|-------|------|---------|
| `uq_drug_synonym_formulation_formulation_id` | UNIQUE | One row per formulation_id |
| `idx_drug_synonym_formulation_formulation_id` | B-tree | Fast FK joins |
| `idx_dsf_synonyms_gin` | GIN | Array containment queries (`synonyms @> ARRAY[...]`) |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `scripts/populate_drug_table.py` | **PREREQUISITE**: drug table must exist with rxcui filled | — | `drugdb.drug` |
| 2 | `scripts/update_drug_rxnorm_columns.py` | **PREREQUISITE**: rxcui column must be filled | — | `drugdb.drug.rxcui` |
| 3 | `scripts/populate_drug_synonym_formulation.py` | Loads rxcui→formulation_id map, extracts synonyms[] from each rxnorm entry, inserts one row per formulation | `DrugMasterLinkage.combined_clean_jsonb rxnorm[].synonyms` | `drugdb.drug_synonym_formulation` (~66K rows) |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `scripts/populate_drug_synonym_formulation.py` | Python | Core: extracts synonyms from rxnorm entries, matches via rxcui→formulation_id |
| `logs/synonym_population.log` | Log | Full population run tracking (2026-05-02: built all synonym rows) |
| `logs/synonym_dryrun.log` | Log | Dry-run verification before actual population |

### Notes for New Contributors

- **Dependency**: `drugdb.drug.rxcui` must be 100% populated before running this script. Run `update_drug_rxnorm_columns.py` first.
- The script is **resume-safe**: it only loads formulation_ids not yet in `drug_synonym_formulation` (uses `NOT EXISTS` subquery).
- To verify coverage: `SELECT COUNT(DISTINCT formulation_id) FROM drugdb.drug_synonym_formulation;` — should be ~66,000.

---

## Table: drugdb.drug_ingredient_mapping

**Phase:** Phase 1 (populated after drug table + rxcui fill + drugdb.ingredients)  
**Schema:** `public`  
**Rows:** 98,832  
**Coverage:** 99.84% of formulations  
**Purpose:** Maps each drug formulation to its active ingredients. Bridges `drugdb.drug` and `drugdb.ingredients`. Records the mass (amount) and unit for each ingredient in a formulation.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `formulation_id` | UUID FK → drug | The drug formulation |
| `ingredient_id` | UUID FK → drugdb.ingredients | The ingredient |
| `mass` | NUMERIC | Quantity of the ingredient |
| `unit` | VARCHAR(50) | Unit of mass (e.g. "MG", "MCG", "ML") |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `schemas/ingredient_schema.sql` | **PREREQUISITE**: drugdb.ingredients must exist | — | `drugdb.ingredients` (DDL) |
| 2 | `schemas/drugdb_migration.sql` | **PREREQUISITE**: drugdb.ingredients must be populated | — | `drugdb.ingredients` (19,842 rows) |
| 3 | `scripts/populate_drug_table.py` | **PREREQUISITE**: drug table must exist | — | `drugdb.drug` (88,983 rows) |
| 4 | `scripts/update_drug_rxnorm_columns.py` | **PREREQUISITE**: rxcui must be filled (100% required) | — | `drugdb.drug.rxcui` |
| 5 | `scripts/populate_drug_ingredient_mapping.py` | Core: matches rxcui→formulation_id and ingredient name→ingredient_id; inserts mapping rows | `DrugMasterLinkage rxnorm[].ingredients[]` | `drugdb.drug_ingredient_mapping` (98,832 rows) |
| 6 | `second_pass_ingredient_mapping.py` | Second pass: re-attempts failed ingredient lookups with alternate strategies | `drugdb.drug_ingredient_mapping` gaps | Additional mapping rows |
| 7 | `logs/drug_ingredient_mapping_second_pass.log` | Log: records outcome of second pass | — | Log file |
| 8 | `logs/drug_ingredient_mapping_second_pass.json` | JSON: structured results of second pass | — | JSON results file |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `scripts/populate_drug_ingredient_mapping.py` | Python | Core: extracts ingredients from rxnorm entries, matches rxcui + ingredient name |
| `second_pass_ingredient_mapping.py` | Python | Second pass: handles unmatched ingredients from first pass |
| `data/fix_drug_ingredient_mapping.py` | Python | Data repair: fixes specific wrong mappings identified in QA |
| `logs/ingredient_mapping.log` | Log | First-pass population run (2026-05-02: 88,983 formulations, ~99.84% coverage) |
| `logs/drug_ingredient_mapping_second_pass.log` | Log | Second-pass population run tracking |
| `logs/drug_ingredient_mapping_second_pass.json` | JSON | Structured second-pass match results |
| `logs/fuzzy_ingredient_match_results.json` | JSON | Results from fuzzy matching of unresolved ingredients |
| `logs/fuzzy_ingredient_match_apply_log.txt` | Text | Log from applying fuzzy match results |
| `fuzzy_ingredient_dedup.py` | Python | Deduplicates fuzzy-matched ingredient candidates |
| `fuzzy_match_indian_ingredients.py` | Python | Fuzzy-matches Indian brand ingredients against drugdb |
| `fuzzy_match_results.json` | JSON | Raw fuzzy match output |
| `apply_fuzzy_matches.py` | Python | Applies approved fuzzy matches to the mapping table |
| `logs/fuzzy_match_update.log` | Log | Tracks application of fuzzy matches (2026-05-01: found 285 unmapped, 5 updated) |
| `match_statistics.json` | JSON | Summary statistics from matching run |
| `matched_ingredients.txt` | Text | Human-readable list of matched ingredients |
| `scripts/match_statistics.json` | JSON | Match statistics (copy in scripts dir) |

### Notes for New Contributors

**Run order is strict** (from the script's docstring):
1. `populate_drug_table.py` — must run first
2. `update_drug_rxnorm_columns.py` — fills rxcui (100% required)
3. `populate_drug_ingredient_mapping.py` — this script

To check current coverage:
```sql
SELECT 
    COUNT(DISTINCT d.formulation_id) AS total,
    COUNT(DISTINCT dim.formulation_id) AS mapped,
    ROUND(100.0 * COUNT(DISTINCT dim.formulation_id)/COUNT(DISTINCT d.formulation_id), 2) AS pct
FROM drugdb.drug d
LEFT JOIN drugdb.drug_ingredient_mapping dim ON dim.formulation_id = d.formulation_id;
```
Expected: ~99.84%

---

## Table: drugdb.drug_identifier

**Phase:** Phase 5  
**Schema:** `public`  
**Rows:** 578,635  
**Purpose:** Universal identifier lookup table. Maps any external ID (rxcui, NDC product code, NDC package code, UNII, DrugBank ID, UPC, application_number, SPL ID, SPL set_id) to a `formulation_id`. Enables the entity resolver to find a drug from any of its external identifiers.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment |
| `formulation_id` | UUID FK → drug | The drug formulation this identifier belongs to |
| `id_type` | TEXT | One of: `rxcui`, `ndc_product`, `ndc_package`, `unii`, `upc`, `application_number`, `spl_id`, `spl_set_id`, `drugbank` |
| `id_value` | TEXT | The actual identifier string |

### Indexes

| Index | Type | Purpose |
|-------|------|---------|
| `idx_di_lookup` | B-tree (id_type, id_value) | Primary lookup: resolve any ID type + value → formulation_id |
| `idx_di_formulation` | B-tree (formulation_id) | Forward lookup: all IDs for a formulation |
| `idx_di_rxcui` | Partial (id_value WHERE id_type='rxcui') | Fast RxCUI-only lookups |
| `idx_di_ndc` | Partial (id_value WHERE id_type IN ndc_*) | Fast NDC-only lookups |

### JSON Paths Used for Extraction

| id_type | Source JSON Path |
|---------|-----------------|
| `rxcui` | `rxnorm[].rxcui` |
| `ndc_product` | `openfda.openfda_metadata.ndc_product[]` |
| `ndc_package` | `dailymed.drug_info.products[].ndc_code` |
| `unii` | `openfda.openfda_metadata.unii[]` |
| `application_number` | `openfda.openfda_metadata.application_number[]` |
| `spl_id` | `openfda.openfda_metadata.spl_id[]` |
| `spl_set_id` | `openfda.openfda_metadata.spl_set_id[]` |
| `drugbank` | `drugbank[].drugbank_id` |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `schemas/drug_identifier_schema.sql` | DDL: creates the table and indexes | None | Table DDL |
| 2 | `scripts/populate_drug_table.py` | **PREREQUISITE**: drug table must be populated | — | `drugdb.drug` |
| 3 | `scripts/populate_drug_identifier.py` | Core: streams DrugMasterLinkage JOIN drug; extracts all ID types and inserts one row per (formulation_id, id_type, id_value) | `DrugMasterLinkage.combined_clean_jsonb JOIN drugdb.drug` | `drugdb.drug_identifier` (578,635 rows) |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `schemas/drug_identifier_schema.sql` | SQL | DDL for the table, with all indexes and comments |
| `scripts/populate_drug_identifier.py` | Python | Core population script; uses server-side streaming cursor; auto-timestamps log file |
| `logs/drug_identifier_populate_20260505_082038.log` | Log | First run attempt (2026-05-05 08:20) |
| `logs/drug_identifier_populate_20260505_082148.log` | Log | Second run attempt |
| `logs/drug_identifier_populate_20260505_082250.log` | Log | Third run attempt |
| `logs/drug_identifier_populate_20260505_082521.log` | Log | Fourth run attempt |
| `logs/drug_identifier_populate_20260505_082604.log` | Log | Fifth run attempt |
| `logs/drug_identifier_populate_20260505_082636.log` | Log | Sixth run attempt |
| `logs/drug_identifier_populate_20260505_082821.log` | Log | Seventh run attempt |
| `logs/drug_identifier_populate_20260505_082839.log` | Log | Eighth run attempt |
| `logs/drug_identifier_populate_20260505_082854.log` | Log | Ninth run attempt |
| `logs/drug_identifier_populate_20260505_084430.log` | Log | Tenth run attempt |
| `logs/drug_identifier_populate_20260505_084552.log` | Log | Final successful run (2026-05-05 08:45, 578,635 rows inserted) |

> **Note on multiple log files:** The 11 timestamped log files reflect iterative debugging of the strength-matching logic for ndc_product/application_number. Each run auto-generates a new timestamped log at `logs/drug_identifier_populate_<YYYYMMDD_HHMMSS>.log`.

### Notes for New Contributors

- The script is resume-safe: uses `ON CONFLICT (formulation_id, id_type, id_value) DO NOTHING`.
- The NDC strength-matching logic was the main source of iteration — it skips NDC assignments when the strength in the DailyMed product doesn't match the strength of the formulation (to avoid assigning a 100mg NDC to a 500mg formulation).
- To verify: `SELECT id_type, COUNT(*) FROM drugdb.drug_identifier GROUP BY id_type ORDER BY COUNT(*) DESC;`

---

## Table: drugdb.ingredients

**Phase:** Phase 4 (DDL) + Phase 5 (population) + Phase 3 refinement (rxcui backfill + fuzzy dedup)  
**Schema:** `drugdb`  
**Rows:** 20,037 (19,842 from DrugBank migration + 195 skeleton rows from RxNorm-only ingredients)  
**Purpose:** DrugBank ingredient reference data. One row per active pharmaceutical ingredient, with pharmacological details (indications, general function, pharmacodynamics, food interactions, classification). The authoritative source of `drugbank_id` used across the system.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID PK | Generated UUID |
| `drugbank_id` | VARCHAR(50) | DrugBank ID (e.g. `DB00001`) — 99.1% populated |
| `unii` | VARCHAR(50) | FDA UNII identifier |
| `rxcui` | VARCHAR(50) | RxNorm concept ID — 10.7% populated (2,137 of 20,037) |
| `name` | VARCHAR(255) | Ingredient name (NOT NULL) |
| `indications` | TEXT | Clinical indications from DrugBank |
| `general_function` | TEXT | Biological function |
| `type` | ENUM | `active`, `inactive`, or `both` |
| `pharmacodynamics` | TEXT | Pharmacodynamics text |
| `classification_description` | TEXT | ATC/pharmacological classification |
| `food_interactions` | TEXT | Food interaction text (JSON-serialized array) |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `schemas/ingredient_schema.sql` | DDL: creates `drugdb` schema + 3 tables + triggers + indexes | None | `drugdb.ingredients` (DDL) |
| 2 | `schemas/drugdb_migration.sql` | Populates from `DrugSourceMaster.standardized_records WHERE source='drugbank'` | `DrugSourceMaster` (19,842 drugbank rows) | `drugdb.ingredients` (19,842 rows) |
| 3 | `scripts/update_ingredient_rxcui.py` | Backfills rxcui from DrugMasterLinkage rxnorm[].ingredients[].ing_rxcui using 4-method matching | `DrugMasterLinkage rxnorm[].ingredients[]` | `drugdb.ingredients.rxcui` (2,137 updated, 195 skeleton rows inserted) |
| 4 | `fuzzy_ingredient_dedup.py` | Fuzzy-matches 195 skeleton rows against DrugBank rows to fill drugbank_id | `drugdb.ingredients` (skeleton rows) | `drugdb.ingredients.drugbank_id` (5 rows updated on 2026-05-04) |
| 5 | `data/update_drugbank_ids.py` | Updates specific DrugBank IDs from lookup | DrugBank lookup results | `drugdb.ingredients.drugbank_id` |
| 6 | `bulk_update_drugbank_fast.py` | Batch updates DrugBank IDs at scale | Lookup table | `drugdb.ingredients.drugbank_id` |
| 7 | `execute_stage1_updates.py` | Stage 1 orchestration: runs all stage 1 ingredient updates in sequence | Multiple | Multiple updates |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `schemas/ingredient_schema.sql` | SQL | DDL: creates drugdb schema, 3 tables, triggers, indexes |
| `schemas/drugdb_migration.sql` | SQL | Populates all 3 drugdb tables from DrugSourceMaster |
| `scripts/update_ingredient_rxcui.py` | Python | Backfills rxcui on ingredients from DrugMasterLinkage |
| `fuzzy_ingredient_dedup.py` | Python | Deduplicates and fuzzy-matches skeleton ingredient rows |
| `data/drugbank_lookup.py` | Python | Looks up DrugBank IDs from external sources |
| `data/drugbank_lookup.log` | Log | DrugBank lookup run log |
| `data/drugbank_lookup_checkpoint.csv` | CSV | Checkpoint file for resumable DrugBank lookup |
| `data/drugbank_lookup_results.csv` | CSV | Initial DrugBank lookup results |
| `data/drugbank_lookup_results_COMPLETE.csv` | CSV | Complete DrugBank lookup results (all records processed) |
| `data/drugbank_lookup_results_FINAL.csv` | CSV | Final cleaned DrugBank lookup results for import |
| `data/update_drugbank_ids.py` | Python | Applies specific DrugBank ID corrections to ingredients table |
| `bulk_update_drugbank_fast.py` | Python | Batch DrugBank ID updates using executemany for speed |
| `execute_stage1_updates.py` | Python | Orchestrates all stage 1 ingredient update steps |
| `stage1_execution_log.txt` | Text | Log from stage1_updates orchestration |
| `scripts/indian_brand_mapper.py` | Python | Maps Indian brand generics to FDA formulations (references ingredients) |
| `fix_missing_35.py` | Python | Repairs 35 specific missing ingredient records |
| `logs/ingredient_mapping.log` | Log | Ingredient-to-formulation mapping run log |
| `logs/update_ingredient_rxcui_dryrun.log` | Log | Dry-run of rxcui update |
| `logs/update_ingredient_rxcui_dryrun2.log` | Log | Second dry-run of rxcui update |
| `logs/update_ingredient_rxcui_run.log` | Log | Actual rxcui update run (2026-05-02: extracted 2,137 ingredients) |
| `schemas/drugdb_data_quality_report.sql` | SQL | 10-section data quality validation queries (results from 2026-04-30) |
| `reports/CDSS_DATABASE_DEEP_RESEARCH_REPORT.md` | Markdown | Deep analysis of all table statistics and coverage (2026-05-04) |

### Matching Methods for rxcui backfill (`update_ingredient_rxcui.py`)

| Method | Strategy | Example |
|--------|---------|---------|
| Method 1 | Exact name match (case-insensitive) against `ingredients.name` | `abacavir` → rxcui=190521 |
| Method 2 | Prefix name match (shortest ingredient name that starts with the drug name) | `acetohydroxamic acid` → rxcui=16728 |
| Method 3 | Exact synonym match via `ingredient_synonyms.synonym` | `Paracetamol` → via acetaminophen synonym |
| Method 4 | Prefix synonym match | Fallback for unusual names |

### Notes for New Contributors

- The 195 "skeleton rows" are RxNorm-only ingredients that have no DrugBank record — they were inserted as placeholders to maintain FK integrity in `drug_ingredient_mapping`.
- The `rxcui` coverage is only 10.7% because DrugBank does not systematically include RxCUI codes. Only those with matching RxNorm entries get a rxcui.
- To re-run the rxcui backfill: `python3 scripts/update_ingredient_rxcui.py --password Admin@123 --dry-run` first, then without `--dry-run`.

---

## Table: drugdb.ingredient_synonyms

**Phase:** Phase 4 (DDL) + Phase 5 (population)  
**Schema:** `drugdb`  
**Rows:** 52,154  
**Purpose:** Stores all alternate names (synonyms) for each ingredient from the DrugBank source. Used by the entity resolver's synonym-matching tier and by `drug_ingredient_mapping` to find ingredients whose primary name doesn't match.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID FK → ingredients | Links to the parent ingredient |
| `synonym` | VARCHAR(500) | The alternate name |
| `created_at` | TIMESTAMPTZ | Auto-filled |
| `updated_at` | TIMESTAMPTZ | Auto-updated via trigger |
| `created_by` | VARCHAR(255) | Audit field |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `schemas/ingredient_schema.sql` | DDL: creates table, FK, trigger, indexes | None | `drugdb.ingredient_synonyms` (DDL) |
| 2 | `schemas/drugdb_migration.sql` | Populates: extracts synonyms array from each DrugBank record | `DrugSourceMaster.standardized_records->'drug_info'->'synonyms'[]` | 52,154 rows |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `schemas/ingredient_schema.sql` | SQL | DDL including ingredient_synonyms table and trigger |
| `schemas/drugdb_migration.sql` | SQL | Migration SQL that populates synonyms from standardized_records |
| `logs/rxcui_update.log` | Log | RxCUI update run that also touches synonym lookups |

### Notes for New Contributors

- When adding a new ingredient manually, also insert its synonyms into this table.
- The trigger `trg_update_synonyms` auto-updates `updated_at` on any UPDATE.
- To verify counts: `SELECT COUNT(*) FROM drugdb.ingredient_synonyms;` → expected 52,154.

---

## Table: drugdb.ingredient_interactions

**Phase:** Phase 4 (DDL) + Phase 5 (population)  
**Schema:** `drugdb`  
**Rows:** 2,910,556 (600 dropped due to unresolved DrugBank IDs)  
**Purpose:** Pairwise ingredient interaction matrix from DrugBank. Each row represents a directional interaction between two ingredients (the "subject" and the "reacting" partner). This is the primary source for drug-drug interaction checking in Q2.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID FK → ingredients | The subject ingredient |
| `reacting_id` | UUID FK → ingredients | The partner ingredient that interacts |
| `description` | TEXT | Description of the interaction mechanism |
| `created_at` | TIMESTAMPTZ | Auto-filled |
| `updated_at` | TIMESTAMPTZ | Auto-updated via trigger |
| `created_by` | VARCHAR(255) | Audit field |

> **Known Gap:** 600 interaction rows were intentionally dropped because they referenced two DrugBank IDs (`DB09368` — 542 refs, `DB24348` — 58 refs) that have no record in DrugSourceMaster. These are upstream data gaps.

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `schemas/ingredient_schema.sql` | DDL: creates table, FKs, trigger, indexes | None | `drugdb.ingredient_interactions` (DDL) |
| 2 | `schemas/drugdb_migration.sql` | Populates: for each DrugBank record's `drug_interactions.drug_interactions[]`, resolves `reacting_drugbank_id` → `reacting_id` UUID via DrugSourceMaster.sourceid lookup | `DrugSourceMaster.standardized_records->'drug_interactions'->'drug_interactions'[]` | 2,910,556 rows |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `schemas/ingredient_schema.sql` | SQL | DDL including ingredient_interactions table |
| `schemas/drugdb_migration.sql` | SQL | Migration that populates interactions |
| `schemas/drugdb_data_quality_report.sql` | SQL | Data quality queries including interaction coverage analysis |
| `reports/CDSS_DATABASE_DEEP_RESEARCH_REPORT.md` | Markdown | Documents the 600 dropped interactions |

### Notes for New Contributors

- Interactions are **directional** (ingredient A → B ≠ B → A), though DrugBank often stores both directions.
- To check coverage: `SELECT COUNT(*) FROM drugdb.ingredient_interactions;` → expected 2,910,556.
- The two missing DrugBank IDs (`DB09368`, `DB24348`) are known upstream data gaps. If these are added to DrugBank in the future, re-running `drugdb_migration.sql` will pick them up.

---

## Table: drugdb.indian_brand_ingredient

**Phase:** Phase 3 (DrugBank ID enrichment)  
**Schema:** `drugdb`  
**Rows:** 580,669 (as of 2026-05-01)  
**Purpose:** Stores ingredients from Indian drug brands. Each ingredient has a raw name and a normalized name, and is linked to `drugdb.ingredients` via `drugbank_id` (which is filled by `update_indian_brand_drugbank_id.py`). Used to support Indian brand drug resolution.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment |
| `indian_brand_id` | INT FK → indian_brand | The parent brand |
| `ingredient_name_raw` | TEXT | Raw ingredient name as it appears in Indian brand data |
| `ingredient_name_norm` | TEXT | Normalized ingredient name |
| `drugbank_id` | TEXT | FK to `drugdb.ingredients.drugbank_id` (NULL until filled by update script) |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | Indian brand data load | **PREREQUISITE**: indian_brand_ingredient must be populated | Indian brand source data | `drugdb.indian_brand_ingredient` rows |
| 2 | `scripts/update_indian_brand_drugbank_id.py` | Phase 1 (dry-run): matches ingredient_name_raw/norm against drugdb.ingredients + ingredient_synonyms using 4-tier logic | `drugdb.ingredients`, `drugdb.ingredient_synonyms` | Dry-run report (no writes) |
| 3 | `scripts/update_indian_brand_drugbank_id.py` | Phase 2 (apply): after user confirmation, applies matched drugbank_ids in batches | Matched pairs from Phase 1 | `drugdb.indian_brand_ingredient.drugbank_id` updates |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `scripts/update_indian_brand_drugbank_id.py` | Python | 4-tier matching: exact name, exact synonym, prefix name, prefix synonym |
| `scripts/indian_brand_loader.py` | Python | Loads Indian brand data into `public.indian_brand` and `public.indian_brand_ingredient` (stub) |
| `scripts/indian_brand_mapper.py` | Python | Maps normalized Indian brand generics → FDA formulation_id (stub) |
| `data/files (1)/indian_brand_mapper.py` | Python | Earlier reference version of the Indian brand mapper |
| `data/indian_brands/` | Directory | Indian brand source data files |
| `fuzzy_match_indian_ingredients.py` | Python | Fuzzy-matches Indian brand ingredients against drugdb |
| `logs/indian_brand_drugbank.log` | Log | update_indian_brand_drugbank_id.py run (2026-05-01: found 580,669 NULL records; matched majority via tier 1) |

### Matching Tiers (`update_indian_brand_drugbank_id.py`)

| Tier | Strategy | Notes |
|------|---------|-------|
| Tier 1 | Exact match against `ingredients.name` (case-insensitive) | Tried for raw name first, then norm name |
| Tier 2 | Exact match against `ingredient_synonyms.synonym` (case-insensitive) | Handles brand name variants |
| Tier 3 | Prefix match against `ingredients.name` (shortest wins) | For truncated names |
| Tier 4 | Prefix match against `ingredient_synonyms.synonym` (shortest wins) | Last resort |

---

## Table: public.contraindication

**Purpose:** Stores the raw FDA contraindications JSON object for each drug formulation. Used by Q6 (contraindication safety check). This is a deterministic extract — no LLM or NLP involved.

### Schema

| Column | Type | Notes |
|--------|------|-------|
| `id` | SERIAL PK | Auto-increment |
| `formulation_id` | UUID NOT NULL FK → drug | One row per formulation |
| `source_section` | TEXT | Always `'contraindications'` |
| `contraindications_json` | JSONB | Raw object with `text` (prose) and optionally `table` (array) keys |

### Indexes

| Name | Type | Purpose |
|------|------|---------|
| `contraindication_pkey` | B-tree PK | Row lookup |
| `idx_contra_formulation` | B-tree | Fast FK joins on formulation_id |

### How the table was populated

**Phase:** Phase 2 / Step 2.1f
**Date:** 2026-05-05
**Method:** Direct SQL INSERT — deterministic transform only (no LLM, no NLP)
**Script:** Direct psql (inline SQL, no Python script)
**Log:** `logs/contraindication_load.log`

**Source JSON path:**
```
DrugMasterLinkage.combined_clean_jsonb -> 'openfda' -> 'safety' -> 'contraindications'
```

**INSERT query:**
```sql
INSERT INTO contraindication (formulation_id, source_section, contraindications_json)
SELECT
    d.formulation_id,
    'contraindications',
    dml.combined_clean_jsonb -> 'openfda' -> 'safety' -> 'contraindications'
FROM "DrugMasterLinkage" dml
JOIN drug d ON d.master_linkage_id = dml.master_linkage_id
WHERE dml.combined_clean_jsonb -> 'openfda' -> 'safety' -> 'contraindications' IS NOT NULL
  AND dml.combined_clean_jsonb -> 'openfda' -> 'safety' -> 'contraindications' != 'null'::jsonb;
```

### Row Counts

| Metric | Count |
|--------|-------|
| Total rows inserted | **84,436** |
| Rows with `text` key only | 84,297 |
| Rows with both `text` and `table` keys | 139 |
| Total accounted for | 84,436 |

### Notes

- The entire JSON object is stored as-is — no parsing, unnesting, or transformation.
- `formulation_id` column type is `UUID` (matches `drug.formulation_id`).
- The `table` key (structured tabular contraindication data) is present in 139 rows; all others have `text` only.
- Safe to re-run: table is dropped and recreated on each run.

---

## Table: drugdb.drug_interaction

**Phase:** Phase 5 (DDL + population)
**Schema:** `drugdb`
**Rows:** Unknown (first run pending)
**Purpose:** Pairwise drug-drug interaction table at the formulation level. Each row represents a directional interaction between two drug formulations (subject → partner). Derived from `drugdb.ingredient_interactions` by resolving ingredient UUIDs to formulation IDs via `drugdb.drug_ingredient_mapping`. `severity` and `mechanism` columns are NULL after initial population and are filled by the LLM enrichment phase.

### Key Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PK | Auto-increment surrogate key |
| `interaction_id` | TEXT UNIQUE | `subject_formulation_id || '_' || partner_formulation_id` — synthetic deduplication key |
| `subject_formulation_id` | TEXT FK → drug | The drug being checked (the "object" drug) |
| `partner_formulation_id` | TEXT FK → drug | The drug it interacts with (the "precipitant" drug) |
| `severity` | TEXT | Interaction severity: major/moderate/minor/unknown — **NULL after initial load; populated by LLM layer** |
| `mechanism` | TEXT | Mechanistic explanation of the interaction — **NULL after initial load; populated by LLM layer** |
| `evidence_level` | TEXT | Confidence level; DEFAULT `established` (DrugBank interactions are curated) |
| `source_excerpt` | TEXT | Raw interaction description from `ingredient_interactions.description` — used as LLM input |
| `created_at` | TIMESTAMPTZ | Auto-filled on insert |
| `updated_at` | TIMESTAMPTZ | Updated manually after LLM enrichment |

### Source Tables

| Table | Role |
|-------|------|
| `drugdb.drug_ingredient_mapping` | Maps formulation_id ↔ ingredient_id (92,570 rows; preloaded into memory) |
| `drugdb.ingredient_interactions` | Provides 2,910,556 ingredient-level interaction pairs (streamed via server-side cursor) |
| `drugdb.drug` | Target FK for subject_formulation_id and partner_formulation_id |

### Pipeline (Execution Order)

| Step | File | Purpose | Inputs | Outputs |
|------|------|---------|--------|---------|
| 1 | `schemas/drug_interaction_schema.sql` | DDL: creates table + 4 indexes + column comments | None | `drugdb.drug_interaction` (DDL only) |
| 2 | `scripts/populate_drug_interaction.py` | ETL: streams ingredient_interactions, resolves ingredient UUIDs to formulation IDs, inserts one row per unique pair | `drugdb.drug_ingredient_mapping`, `drugdb.ingredient_interactions` | `drugdb.drug_interaction` rows |

### All Related Files

| File | Type | Reason Created |
|------|------|----------------|
| `schemas/drug_interaction_schema.sql` | SQL | DDL: creates table, 4 indexes, and all column comments |
| `scripts/populate_drug_interaction.py` | Python | ETL: resolves ingredient-level pairs to formulation-level pairs and inserts rows |
| `logs/drug_interaction_population.log` | Log | Default log path for populate_drug_interaction.py runs |

### LLM Enrichment Phase (Pending)

Two columns are intentionally left NULL after initial population:

- **`severity`** — will be set to `major`, `moderate`, `minor`, or `unknown` by the LLM layer using `source_excerpt` as input.
- **`mechanism`** — will be populated with a pharmacological mechanistic explanation by the LLM layer.

The `source_excerpt` column carries the raw DrugBank description text that the LLM will use as context.

### Notes for New Contributors

- Interactions are **directional**: A→B and B→A are separate rows. DrugBank typically stores both directions in `ingredient_interactions`, so both will appear here.
- Rows are skipped when either the subject or partner ingredient has no formulation in `drug_ingredient_mapping` — these are DrugBank-only ingredients with no corresponding FDA/RxNorm formulation.
- The `interaction_id` UNIQUE constraint makes re-runs safe (`ON CONFLICT DO NOTHING`).
- To run: `python3 scripts/populate_drug_interaction.py --password Admin@123 --dry-run` first, then without `--dry-run`.
- To verify: `SELECT COUNT(*) FROM drugdb.drug_interaction;`

---

## Severity Enrichment Pipeline

**Phase:** Phase 5-LLM (runs after Phase 5 drugdb tables are populated)
**Target table:** `drugdb.ingredient_interactions` — adds `severity` and `mechanism` to existing 2,910,556 rows
**LLM model:** Qwen-Flash (Alibaba Cloud DashScope international endpoint)
**Why Qwen-Flash:** Fast, cheap batch-mode model; $0.00057/1K input + $0.0023/1K output; supports 24-hour completion window via the OpenAI-compatible Batch API. Latency is irrelevant here since we use asynchronous batch processing.

### Purpose

Classifies every drug–drug interaction in `drugdb.ingredient_interactions` with:
- **`severity`**: one of `contraindicated | major | moderate | minor | unknown`
- **`mechanism`**: 3–8 word pharmacological mechanism phrase (e.g. "CYP3A4 inhibition", "decreased renal excretion")

Only the 1,455,278 **unique** descriptions are sent to the LLM (A→B and B→A always share the same description text). The B→A row gets its values mirrored via a single SQL UPDATE at the end, at zero LLM cost.

### Prerequisites

| Requirement | Detail |
|-------------|--------|
| Alibaba Cloud API key | DashScope international account — `dashscope-intl.aliyuncs.com` |
| `openai` Python package | `pip install openai` (uses OpenAI-compatible API) |
| `drugdb.ingredient_interactions` populated | Run Phase 5a (`drugdb_migration.sql`) first |
| SQL alter script applied | `psql -f schemas/alter_ingredient_interactions_severity.sql` adds columns |

### Files

| File | Type | Purpose |
|------|------|---------|
| `schemas/alter_ingredient_interactions_severity.sql` | SQL | Adds `severity` (DEFAULT `unknown`) + `mechanism` (NULL) columns + 2 indexes |
| `scripts/enrich_severity_mechanism.py` | Python | 7-stage enrichment pipeline (pre-filter → JSONL → submit → poll → parse → mirror → verify) |
| `data/severity_batches/batch_NNN.jsonl` | JSONL | Generated batch input files (50,000 records each, ~25–30 files total) |
| `logs/enrich_severity_mechanism.log` | Log | Full pipeline log (INFO to console, DEBUG to file) |
| `logs/severity_checkpoint.json` | JSON | Resume checkpoint — saved after every batch file completes |

### Pipeline Stages

| Stage | Method | Description | Rows Affected |
|-------|--------|-------------|---------------|
| 1 | Regex SQL | Pre-filter obvious cases: `%contraindicated%` → contraindicated; `%severe%`/`%fatal%` → major; `%minor%` → minor | ~15% of 2,910,556 |
| 2 | Python | Build JSONL files — DISTINCT ON (description), 50,000 records per file | ~1.24M unique descriptions |
| 3 | Batch API | Upload JSONL files + create batch jobs simultaneously | All files submitted |
| 4 | Poll | Check status every 60s; process each file as it completes | — |
| 5 | Python + DB | Parse LLM JSON output → executemany UPDATE in groups of 5,000 | ~1.24M rows |
| 6 | SQL | Mirror A→B severity+mechanism to B→A rows (single UPDATE, zero LLM cost) | ~1.455M rows |
| 7 | SQL | Final verification count by severity + mechanism coverage | All 2,910,556 rows |

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--api-key` | required | Alibaba Cloud DashScope API key |
| `--db-password` | required | PostgreSQL password |
| `--dry-run` | false | Show 10 sample records + cost estimates; no API calls, no DB writes |
| `--skip-prefilter` | false | Skip Stage 1 regex pre-filter |
| `--skip-mirror` | false | Skip Stage 6 SQL mirror |
| `--batch-size` | 50000 | Records per JSONL file |
| `--poll-interval` | 60 | Seconds between batch status checks |
| `--max-retries` | 3 | Retries per failed batch file (resubmits entire file) |
| `--log-file` | `logs/enrich_severity_mechanism.log` | Log file path |
| `--checkpoint` | `logs/severity_checkpoint.json` | Checkpoint file path |
| `--resume` | false | Resume from existing checkpoint (skips already-completed stages) |

### How to Run

```bash
# Step 1: Add columns (one-time DDL)
psql -h 178.236.185.230 -U postgres -d postgres \
    -f schemas/alter_ingredient_interactions_severity.sql

# Step 2: Dry run — zero cost, shows 10 sample records + cost estimate
python3 scripts/enrich_severity_mechanism.py \
    --api-key YOUR_ALIBABA_KEY \
    --db-password Admin@123 \
    --dry-run

# Step 3: Full run via nohup (survives terminal close)
nohup python3 scripts/enrich_severity_mechanism.py \
    --api-key YOUR_ALIBABA_KEY \
    --db-password Admin@123 \
    --log-file logs/enrich_severity_mechanism.log \
    > logs/enrich_nohup.log 2>&1 &
echo "PID: $!"

# Step 4: Monitor
tail -f logs/enrich_severity_mechanism.log

# Step 5: Resume if interrupted
python3 scripts/enrich_severity_mechanism.py \
    --api-key YOUR_ALIBABA_KEY \
    --db-password Admin@123 \
    --resume \
    --checkpoint logs/severity_checkpoint.json
```

### Expected Output Counts

| Metric | Expected |
|--------|---------|
| Total rows in table | 2,910,556 |
| Unique descriptions | 1,455,278 |
| Stage 1 pre-filtered | ~430,000 (est. ~15%) |
| Sent to LLM | ~1,025,278 (est.) |
| Rows mirrored (Stage 6) | ~1,025,278 (est.) |
| Final classified | ~2,800,000+ (est. >96%) |
| Still unknown | <110,000 (est. <4%) |

### Cost Estimate

| Item | Estimate |
|------|---------|
| Records sent to LLM | ~1,025,278 |
| Input tokens (avg 150/record) | ~154M tokens |
| Output tokens (avg 25/record) | ~25.6M tokens |
| Cost @ $0.00057/1K input | ~$87.80 |
| Cost @ $0.0023/1K output | ~$58.90 |
| **Total estimated cost** | **~$147** |
| Batch API time | 2–8 hours (varies by queue) |

### Resume / Checkpoint Behavior

The checkpoint file (`logs/severity_checkpoint.json`) is written atomically (via `.tmp` swap) after every batch file completes. On `--resume`:
- Files already in `completed[]` are skipped in Stage 5
- Files already in `submitted[]` skip Stage 3 re-upload
- Failed files are retried automatically up to `--max-retries` times before being logged as permanently failed

### Notes for New Contributors

- The `severity` column DEFAULT is `'unknown'` — all rows start unclassified. The pipeline is safe to re-run; already-classified rows are excluded by `WHERE severity = 'unknown'` in Stages 1 and 2.
- The pre-filter runs in order: contraindicated first, then major, then minor. Each stage only touches `WHERE severity = 'unknown'`, so there is no double-classification.
- The DISTINCT ON query in Stage 2 may take 20–40 minutes on 2.9M rows — this is a one-time sequential scan + sort and is expected.
- To verify current state: `SELECT severity, COUNT(*) FROM drugdb.ingredient_interactions GROUP BY severity ORDER BY COUNT(*) DESC;`

---

## Pending Tables (Phase 2+)

The following tables have DDL defined in `schemas/postgres_schema.sql` but have **zero rows** as of 2026-05-05. They will be populated by LLM-assisted extraction (Phase 2), RAG embedding (Phase 3), and Neo4j graph population (Phase 4).

| Table | Schema | Rows | Phase to populate | Unblocks |
|-------|--------|------|------------------|---------|
| `public.active_ingredient` | public | 0 | Phase 2 (Pass 2 extraction) | Q9 (pill burden) |
| `public.inactive_ingredient` | public | 0 | Phase 2 (Pass 2 extraction) | — |
| `public.drug_indication` | public | 0 | Phase 2 (Pass 2 extraction) | Q1, Q3, Q5, Q6 |
| `public.drug_interaction` | public | 0 | Phase 2 (Pass 2 extraction) | Q2, Q6 |
| `public.contraindication` | public | 84436 | Phase 2 Step 2.1f (deterministic extract) — **DONE 2026-05-05** | Q6 |
| `public.dosing_regimen` | public | 0 | Phase 2 (Pass 2 extraction) | Q4, Q7 |
| `public.population_approval` | public | 0 | Phase 2 (Pass 2 extraction) | Q5 |
| `public.administration_timing` | public | 0 | Phase 2 (Pass 2 extraction) | Q8 |
| `public.adverse_event` | public | 0 | Phase 2 (Pass 2 extraction) | Future |
| `public.warning` | public | 0 | Phase 2 (Pass 2 extraction) | Future |
| `public.clinical_section` | public | 0 | Phase 2 (Pass 2 extraction) | All Q RAG |
| `public.label_table` | public | 0 | Phase 2 (Pass 2 extraction) | Q4, Q7 |
| `public.rag_chunk` | public | 0 | Phase 3 (embedding) | Vector search |
| `public.rxnorm_formulation` | public | 0 | Phase 2 (Pass 2 extraction) | Q9 |
| `public.indian_brand` | public | 0 | Phase 6 (Indian brands) | Indian brand Q |
| `public.indian_brand_ingredient` | public | 0 | Phase 6 (Indian brands) | Indian brand Q |
| `public.query_audit_log` | public | 0 | Runtime (API) | Audit trail |

### Scripts for Pending Tables

| File | Purpose | Status |
|------|---------|--------|
| `scripts/pass2_extractors.py` | 9 LLM extractors for structured facts (strength, indication, drug_class, etc.) | Reference implementation — NOT deployed |
| `scripts/run_pass2.py` | Orchestrates Pass 2 batch LLM extraction against all unified records | NOT deployed |
| `scripts/chunk_for_rag.py` | Splits clinical_section text into ~500-token chunks for embedding | NOT deployed |
| `scripts/embed_chunks.py` | Embeds chunks using BAAI/bge-large-en-v1.5 (1024-dim vectors) via TEI | NOT deployed |
| `scripts/neo4j_populate.py` | Populates Neo4j graph nodes and relationships | NOT deployed |
| `scripts/indian_brand_loader.py` | Loads Indian brand source data into indian_brand tables | Stub (empty file) |
| `scripts/indian_brand_mapper.py` | Maps Indian brand generics to FDA formulations | Stub |
| `data/files (1)/transform_to_unified.py` | Transforms DrugMasterLinkage JSONB → unified record format | NOT deployed |
| `scripts/transform_to_unified.py` | Same — production version | NOT deployed |

---

## Cross-Table Dependencies

```
DrugSourceMaster (input)
    │
    ├──[Phase 1-3: Schema Extraction/Normalization/Standardization]──→ DrugSourceMaster.standardized_records
    │
    ├──[Phase 4+5: drugdb_migration.sql]
    │       ├──→ drugdb.ingredients (19,842 DrugBank rows)
    │       │       └──[update_ingredient_rxcui.py]──→ +195 skeleton rows; rxcui backfilled on 2,137
    │       ├──→ drugdb.ingredient_synonyms (52,154 rows)
    │       └──→ drugdb.ingredient_interactions (2,910,556 rows)
    │
    └──[Via DrugMasterLinkage.combined_clean_jsonb]
            │
            ├──[populate_drug_table.py]──→ drugdb.drug (88,983 rows)
            │       │
            │       ├──[update_drug_rxnorm_columns.py]──→ drug.rxcui + drug.rxnorm_generic_formulation
            │       ├──[update_drug_new_columns.py]──→ drug.product_type, routes, mechanism_of_action, has_*
            │       │
            │       ├──[populate_drug_synonym_formulation.py]──→ drugdb.drug_synonym_formulation (~66K rows)
            │       │
            │       ├──[populate_drug_identifier.py]──→ drugdb.drug_identifier (578,635 rows)
            │       │
            │       └──[populate_drug_ingredient_mapping.py]──→ drugdb.drug_ingredient_mapping (98,832 rows)
            │               │
            │               └── requires: drugdb.ingredients (FK) ──────────────────────────────────┐
            │                       │                                                               │
            │                       └──[populate_drug_interaction.py]──→ drugdb.drug_interaction    │
            │                               requires: drugdb.ingredient_interactions ───────────────┘
            │
            └──[PENDING] Pass 2 extraction
                    ├──→ public.drug_indication
                    ├──→ public.drug_interaction
                    ├──→ public.dosing_regimen
                    ├──→ public.population_approval
                    ├──→ public.administration_timing
                    ├──→ public.active_ingredient
                    ├──→ public.inactive_ingredient
                    ├──→ public.clinical_section
                    └──→ public.rag_chunk (via chunk_for_rag.py → embed_chunks.py)
```

### Foreign Key Chain

```
drugdb.drug (formulation_id PK)
  ← drugdb.drug_synonym_formulation.formulation_id
  ← drugdb.drug_identifier.formulation_id
  ← drugdb.drug_ingredient_mapping.formulation_id
  ← public.drug_indication.formulation_id
  ← public.drug_interaction.subject_formulation_id
  ← public.dosing_regimen.formulation_id
  ← public.population_approval.formulation_id
  ← public.administration_timing.formulation_id
  ← public.active_ingredient.formulation_id
  ← public.inactive_ingredient.formulation_id
  ← public.clinical_section.formulation_id
  ← public.rag_chunk.formulation_id
  ← public.indian_brand.formulation_id (nullable FK)

drugdb.drug_interaction (interaction_id UNIQUE)
  subject_formulation_id FK → drugdb.drug.formulation_id
  partner_formulation_id FK → drugdb.drug.formulation_id
  (populated from ingredient_interactions + drug_ingredient_mapping)

drugdb.ingredients (id PK)
  ← drugdb.drug_ingredient_mapping.ingredient_id
  ← drugdb.ingredient_synonyms.id
  ← drugdb.ingredient_interactions.id
  ← drugdb.ingredient_interactions.reacting_id
  ← drugdb.indian_brand_ingredient.drugbank_id (soft, via drugbank_id text)
```

---

## Full Pipeline Sequence (All Phases)

### Phase 0 — Day 0 (Pre-existing — not created by this pipeline)

The following tables already exist when this pipeline starts:
- `public.DrugSourceMaster` — 738,197 source records from 4 sources
- `public.DrugMasterLinkage` — consolidated linkage table with `combined_clean_jsonb`

### Phase 1 — Schema Extraction (5 minutes)

```bash
python scripts/extract_schema_openfda.py    # → data/master_schema_openfda.json (163 fields)
python scripts/extract_schema_dailymed.py   # → data/master_schema_dailymed.json (4 structural keys)
python scripts/extract_drugbank_schema.py   # → data/master_schema_drugbank.json (11 fields)
```

### Phase 2 — Schema Normalization (2 minutes)

```bash
python scripts/normalize_openfda.py    # → data/normalized_schema.json (13 categories)
python scripts/normalize_dailymed.py   # → data/master_schema_dailymed_normalized.json (10 categories)
python scripts/normalize_drugbank.py   # → data/master_schema_drugbank_normalized.json (4 categories)
```

### Phase 3 — Data Standardization (45–90 minutes)

```bash
python scripts/populate_standardized.py   # openfda (256,165) + dailymed (51,731) → standardized_records
python scripts/populate_remaining.py      # drugbank (19,842) + rxnorm (410,459) → standardized_records
# Total: 738,197 rows in DrugSourceMaster.standardized_records
```

### Phase 4 — Target Schema Creation (< 1 minute)

```bash
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/ingredient_schema.sql
# Creates: drugdb.ingredients, drugdb.ingredient_synonyms, drugdb.ingredient_interactions
# Also: drugdb schema, update_timestamp() function, ENUM type, all indexes + triggers
```

### Phase 5 — Database Population (10–15 minutes for drugdb migration; 30–60 min for drug tables)

```bash
# 5a. Populate DrugBank reference tables
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/drugdb_migration.sql
# → drugdb.ingredients: 19,842 rows
# → drugdb.ingredient_synonyms: 52,154 rows
# → drugdb.ingredient_interactions: 2,910,556 rows

# 5b. Create and populate drugdb.drug (88,983 rows)
python3 scripts/populate_drug_table.py --password Admin@123 --log-file logs/populate_drug_table.log
# Step 5b-1: adds rxcui + rxnorm_generic_formulation columns
psql -h 178.236.185.230 -U postgres -d postgres -f scripts/add_rxnorm_columns.sql
# Step 5b-2: fills those columns
python3 scripts/update_drug_rxnorm_columns.py --password Admin@123 --log-file logs/rxnorm_update.log
# Step 5b-3: adds 9 enrichment columns (DDL)
psql -h 178.236.185.230 -U postgres -d postgres -f scripts/alter_drug_table_new_columns.sql
# Step 5b-4: fills 9 enrichment columns
python3 scripts/update_drug_new_columns.py --password Admin@123 --log-file logs/drug_enrichment.log

# 5c. Populate drug_synonym_formulation
python3 scripts/populate_drug_synonym_formulation.py --password Admin@123 --verify --log-file logs/synonym_population.log

# 5d. Populate drug_ingredient_mapping
python3 scripts/populate_drug_ingredient_mapping.py --password Admin@123 --verify --log-file logs/ingredient_mapping.log

# 5e. Populate drug_identifier (578,635 rows)
python3 scripts/populate_drug_identifier.py --password Admin@123

# 5f. Backfill rxcui on drugdb.ingredients (2,137 rows)
python3 scripts/update_ingredient_rxcui.py --password Admin@123 --log-file logs/update_ingredient_rxcui_run.log

# 5g. Fuzzy-dedup skeleton ingredient rows
python3 fuzzy_ingredient_dedup.py --password Admin@123

# 5h. Update Indian brand ingredient drugbank_ids
python3 scripts/update_indian_brand_drugbank_id.py --password Admin@123

# 5i. Create and populate drugdb.drug_interaction
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/drug_interaction_schema.sql
python3 scripts/populate_drug_interaction.py --password Admin@123 --log-file logs/drug_interaction_population.log

# 5j. Verify all phases
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/verification_queries.sql
```

### Phase 5-LLM — Severity/Mechanism Enrichment of ingredient_interactions

```bash
# Step 1: Add columns (one-time DDL)
psql -h 178.236.185.230 -U postgres -d postgres \
    -f schemas/alter_ingredient_interactions_severity.sql

# Step 2: Dry run first
python3 scripts/enrich_severity_mechanism.py \
    --api-key YOUR_ALIBABA_KEY --db-password Admin@123 --dry-run

# Step 3: Full run
nohup python3 scripts/enrich_severity_mechanism.py \
    --api-key YOUR_ALIBABA_KEY --db-password Admin@123 \
    --log-file logs/enrich_severity_mechanism.log \
    > logs/enrich_nohup.log 2>&1 &
# Enriches: drugdb.ingredient_interactions.severity + mechanism (~1.24M LLM calls + SQL mirror)
# Est. cost: ~$147 | Est. time: 3–10 hours
```

### Phase 6 — Pending: LLM-Assisted Extraction (18–24 hours, 72B model)

```bash
# Requires vLLM service running with Qwen2.5-72B-Instruct-AWQ
# (currently commented out in docker-compose.yml)
python scripts/run_pass2.py    # runs pass2_extractors.py against all unified records
# Populates: drug_indication, drug_interaction, dosing_regimen, population_approval,
#            administration_timing, active_ingredient, inactive_ingredient,
#            clinical_section, label_table
```

### Phase 7 — Pending: RAG Embedding

```bash
python scripts/chunk_for_rag.py   # splits clinical_section text into ~500-token chunks
python scripts/embed_chunks.py    # embeds with BAAI/bge-large-en-v1.5 → rag_chunk.embedding (vector(1024))
```

### Phase 8 — Pending: Neo4j Graph Population

```bash
python scripts/neo4j_populate.py   # populates 7 node types + 9 relationship types
# Requires: Neo4j 5.x running (see docker-compose.yml)
```

### Phase 9 — Pending: Indian Brand Loading

```bash
python scripts/indian_brand_loader.py   # loads indian_brand + indian_brand_ingredient
python scripts/indian_brand_mapper.py   # fuzzy-matches normalized generics → FDA formulation_id
```

### One-Command Full Pipeline

```bash
bash run_all.sh
# Or manually: see PIPELINE.md for the complete phase-by-phase command list
```

---

## Complete File Inventory

### SQL Schema Files (`schemas/`)

| File | Tables Affected | Phase | Purpose |
|------|----------------|-------|---------|
| `schemas/postgres_schema.sql` | All 20 tables | Design | Master CDSS schema DDL (postgres_schema with pgvector, pg_trgm, triggers, entity resolver function) |
| `schemas/postgres_schema_patch.sql` | drug_interaction, active_ingredient, inactive_ingredient, product_sku, ingredient_synonym | Design | Adds critical missing fields (partner_kind, physical characteristics, DrugBank text fields) |
| `schemas/create_drug_table.sql` | `drugdb.drug` | Phase 1 | DDL helper: creates base drug table from DrugMasterLinkage |
| `schemas/drug_identifier_schema.sql` | `drugdb.drug_identifier` | Phase 5 | DDL with full indexes and column comments |
| `schemas/ingredient_schema.sql` | `drugdb.ingredients`, `drugdb.ingredient_synonyms`, `drugdb.ingredient_interactions` | Phase 4 | Creates drugdb schema, ENUM type, 3 tables, triggers, indexes |
| `schemas/drugdb_migration.sql` | `drugdb.*` | Phase 5 | Extracts from standardized_records → populates all 3 drugdb tables |
| `schemas/drugdb_data_quality_report.sql` | All tables | Analysis | 10-section DQR queries with 2026-04-30 reference results |
| `schemas/drug_interaction_schema.sql` | `drugdb.drug_interaction` | Phase 5 | DDL: creates table, 4 indexes, and column comments for all 10 columns |
| `schemas/alter_ingredient_interactions_severity.sql` | `drugdb.ingredient_interactions` | Phase 5-LLM | Adds severity (DEFAULT unknown) + mechanism (NULL) columns + idx_ii_severity + idx_ii_description_hash |
| `schemas/neo4j_schema.cypher` | Neo4j graph | Future | 7 node types, 9 relationship types, 7 uniqueness constraints |
| `schemas/verification_queries.sql` | All tables | All phases | Read-only verification queries for all 5 phases |

### SQL Script Files (`scripts/`)

| File | Tables Affected | Phase | Purpose |
|------|----------------|-------|---------|
| `scripts/add_rxnorm_columns.sql` | `drugdb.drug` | Phase 5 | Adds rxnorm_generic_formulation and rxcui columns + indexes |
| `scripts/alter_drug_table_new_columns.sql` | `drugdb.drug` | Phase 5 | Adds 9 enrichment columns (product_type, routes, mechanism_of_action, has_*) |

### Python Scripts (`scripts/`)

| File | Tables Affected | Phase | Purpose |
|------|----------------|-------|---------|
| `scripts/extract_schema_openfda.py` | DrugSourceMaster | 1 | Extracts all unique OpenFDA field paths |
| `scripts/extract_schema_dailymed.py` | DrugSourceMaster | 1 | Extracts all unique DailyMed field paths |
| `scripts/extract_drugbank_schema.py` | DrugSourceMaster | 1 | Extracts all unique DrugBank field paths |
| `scripts/normalize_openfda.py` | none (file output) | 2 | Reorganizes OpenFDA into 13 semantic categories |
| `scripts/normalize_dailymed.py` | none (file output) | 2 | Reorganizes DailyMed into 10 semantic categories |
| `scripts/normalize_drugbank.py` | none (file output) | 2 | Reorganizes DrugBank into 4 semantic categories |
| `scripts/populate_standardized.py` | DrugSourceMaster | 3 | Fills standardized_records for openfda + dailymed |
| `scripts/populate_remaining.py` | DrugSourceMaster | 3 | Fills standardized_records for drugbank + rxnorm |
| `scripts/populate_drug_table.py` | `drugdb.drug` | 5 | Core: creates + inserts 88,983 drug rows |
| `scripts/update_drug_rxnorm_columns.py` | `drugdb.drug` | 5 | Fills rxcui + rxnorm_generic_formulation via UUID5 seed matching |
| `scripts/update_drug_new_columns.py` | `drugdb.drug` | 5 | Fills 9 enrichment columns from combined_clean_jsonb |
| `scripts/populate_drug_synonym_formulation.py` | `drugdb.drug_synonym_formulation` | 5 | Extracts synonyms from rxnorm entries, matches via rxcui |
| `scripts/populate_drug_ingredient_mapping.py` | `drugdb.drug_ingredient_mapping` | 5 | Maps rxcui→formulation_id + ingredient name→ingredient_id |
| `scripts/populate_drug_identifier.py` | `drugdb.drug_identifier` | 5 | Extracts all external IDs and inserts 578,635 identifier rows |
| `scripts/populate_drug_interaction.py` | `drugdb.drug_interaction` | 5 | Streams ingredient_interactions, resolves to formulation-level pairs, inserts rows |
| `scripts/enrich_severity_mechanism.py` | `drugdb.ingredient_interactions` | 5-LLM | 7-stage pipeline: regex pre-filter → JSONL build → Alibaba Batch API → parse → mirror → verify |
| `scripts/update_ingredient_rxcui.py` | `drugdb.ingredients` | 5 | Backfills rxcui using 4-method matching from DrugMasterLinkage |
| `scripts/update_indian_brand_drugbank_id.py` | `drugdb.indian_brand_ingredient` | 3 | 4-tier matching of Indian brand ingredients → drugbank_id |
| `scripts/fix_rxnorm_uncleaned_rows.py` | `drugdb.drug` | 5 | Fixes rows where rxnorm column wasn't properly cleaned |
| `scripts/indian_brand_loader.py` | `public.indian_brand` | 6 | Stub: will load Indian brand data |
| `scripts/indian_brand_mapper.py` | `public.indian_brand` | 6 | Stub: will map Indian brand generics → formulation_id |
| `scripts/verify_schema.py` | DrugSourceMaster | Utility | Read-only: validates schema overlay logic |
| `scripts/standardize_records.py` | DrugSourceMaster | Superseded | Earlier Phase 3 draft (replaced by populate_standardized.py) |
| `scripts/compare_records.py` | DrugSourceMaster | Utility | Compares records across sources |
| `scripts/postgres_loader.py` | Various | Utility | Generic PostgreSQL data loader utility |
| `scripts/pass2_extractors.py` | All structured facts tables | 6 (pending) | 9 LLM extractors (NOT deployed) |
| `scripts/run_pass2.py` | All structured facts tables | 6 (pending) | Pass 2 orchestration (NOT deployed) |
| `scripts/transform_to_unified.py` | DrugMasterLinkage | 6 (pending) | Converts JSONB → unified record (NOT deployed) |
| `scripts/chunk_for_rag.py` | `public.rag_chunk` | 7 (pending) | Splits clinical text into RAG chunks (NOT deployed) |
| `scripts/embed_chunks.py` | `public.rag_chunk` | 7 (pending) | Embeds chunks into 1024-dim vectors (NOT deployed) |
| `scripts/neo4j_populate.py` | Neo4j graph | 8 (pending) | Populates graph nodes + relationships (NOT deployed) |
| `scripts/populate_standardized.py` | DrugSourceMaster | 3 | Phase 3 standardization for openfda + dailymed |
| `scripts/populate_remaining.py` | DrugSourceMaster | 3 | Phase 3 standardization for drugbank + rxnorm |
| `scripts/cdss_query_templates.py` | All tables | Reference | 9 query templates Q1–Q9 (reference implementation, not deployed) |
| `scripts/extract_schema_openfda.py` | DrugSourceMaster | 1 | Schema extraction |
| `scripts/extract_schema_dailymed.py` | DrugSourceMaster | 1 | Schema extraction |
| `scripts/extract_drugbank_schema.py` | DrugSourceMaster | 1 | Schema extraction |

### Root-Level Python Scripts

| File | Tables Affected | Phase | Purpose |
|------|----------------|-------|---------|
| `apply_fuzzy_matches.py` | `drugdb.drug_ingredient_mapping` | 5 | Applies approved fuzzy match results to ingredient mapping |
| `build_dosage_mappings.py` | none (file output) | 5 | Built the dosage form suffix map (baked into populate_drug_table.py) |
| `bulk_update_drugbank_fast.py` | `drugdb.ingredients` | 5 | Batch DrugBank ID updates using executemany |
| `execute_stage1_updates.py` | `drugdb.ingredients` | 5 | Orchestrates all stage 1 ingredient update steps |
| `export_durg_master_linkage.py` | DrugMasterLinkage | Utility | Exports combined_clean_jsonb to pretty-printed text files |
| `fix_missing_35.py` | `drugdb.ingredients` | 5 | Repairs 35 specific missing ingredient records |
| `fuzzy_ingredient_dedup.py` | `drugdb.ingredients` | 5 | Fuzzy-deduplicates skeleton ingredient rows; fills drugbank_id |
| `fuzzy_match_indian_ingredients.py` | `drugdb.indian_brand_ingredient` | 3 | Fuzzy-matches Indian brand ingredients vs drugdb |
| `second_pass_ingredient_mapping.py` | `drugdb.drug_ingredient_mapping` | 5 | Second pass for failed ingredient lookups |
| `verify_dosage_cleanup.py` | `drugdb.drug` | 5 | Verifies dosage form suffix stripping |

### Data Files (`data/`)

| File | Type | Purpose |
|------|------|---------|
| `data/master_schema_openfda.json` | JSON | Phase 1 output: 163 OpenFDA field paths |
| `data/master_schema_dailymed.json` | JSON | Phase 1 output: DailyMed structure |
| `data/master_schema_drugbank.json` | JSON | Phase 1 output: 11 DrugBank fields |
| `data/normalized_schema.json` | JSON | Phase 2 output: OpenFDA in 13 categories |
| `data/master_schema_dailymed_normalized.json` | JSON | Phase 2 output: DailyMed in 10 categories |
| `data/master_schema_drugbank_normalized.json` | JSON | Phase 2 output: DrugBank in 4 categories |
| `data/field_mapping_openfda.json` | JSON | Raw → normalized field mapping for OpenFDA |
| `data/field_mapping_dailymed.json` | JSON | Raw → normalized field mapping for DailyMed |
| `data/drugbank_field_mapping.json` | JSON | Raw → normalized field mapping for DrugBank |
| `data/dailymed_field_mapping.json` | JSON | DailyMed field mapping (alternate version) |
| `data/normalization_stats.json` | JSON | Stats from normalization run |
| `data/normalization_stats_drugbank.json` | JSON | DrugBank normalization stats |
| `data/transformation_samples.json` | JSON | Before/after sample records (10 per source) |
| `data/sample_rows.json` | JSON | First 2 raw rows from last verify_schema run |
| `data/sample_master_schema.json` | JSON | Schema from verify_schema sample run |
| `data/comparison_detail.json` | JSON | Field-level comparison across sources |
| `data/drugbank_field_catalog.txt` | Text | Human-readable DrugBank field catalog |
| `data/drugbank_normalization_report.txt` | Text | DrugBank normalization run report |
| `data/data-1777879031060.csv` | CSV | [INFERRED] Raw export from database query |
| `data/schema_stats_dailymed.json` | JSON | DailyMed field statistics |
| `data/schema_stats_drugbank.json` | JSON | DrugBank field statistics |
| `data/normalized_schema_dailymed.json` | JSON | DailyMed normalized schema (alternate variant) |
| `data/normalized_schema_drugbank.json` | JSON | DrugBank normalized schema (alternate variant) |
| `data/normalized_schema_openfda.json` | JSON | OpenFDA normalized schema (alternate variant) |
| `data/sample_master_schema.json` | JSON | Master schema sample |
| `data/sample_rows.json` | JSON | Sample database rows |
| `data/sample_transformations.json` | JSON | Sample transformation results |
| `data/patch_16_reds.json` | JSON | [INFERRED] Patch data for 16 records |
| `data/final_complete_patch.py` | Python | Final complete data patch script |
| `data/final_corrections_patch.json` | JSON | Final corrections patch data |
| `data/final_patch_for_gcp.py` | Python | Final patch adapted for GCP deployment |
| `data/final_verified_patch.py` | Python | Verified final patch |
| `data/fix_drug_ingredient_mapping.py` | Python | Data repair for drug_ingredient_mapping |
| `data/update_drugbank_ids.py` | Python | Updates specific DrugBank IDs |
| `data/drugbank_lookup.py` | Python | Looks up DrugBank IDs from external sources |
| `data/drugbank_lookup.log` | Log | DrugBank lookup run log |
| `data/drugbank_lookup_checkpoint.csv` | CSV | Resumable checkpoint for DrugBank lookup |
| `data/drugbank_lookup_results.csv` | CSV | Initial DrugBank lookup results |
| `data/drugbank_lookup_results_COMPLETE.csv` | CSV | All-records DrugBank lookup results |
| `data/drugbank_lookup_results_FINAL.csv` | CSV | Final cleaned DrugBank lookup results |
| `data/verification_report.txt` | Text | Verification results report |

### Data Files — Reference Archive (`data/files (1)/`)

> These are earlier draft/reference versions of scripts and schemas, kept for historical context.

| File | Type | Purpose |
|------|------|---------|
| `data/files (1)/00_INDEX.md` | Markdown | Index of reference files |
| `data/files (1)/01_PRD_OVERVIEW.md` | Markdown | PRD (earlier version) |
| `data/files (1)/02_REST_API_DESIGN.md` | Markdown | REST API design (earlier version) |
| `data/files (1)/03_PROMPT_TEMPLATES.md` | Markdown | LLM prompt templates (earlier version) |
| `data/files (1)/04_TEST_CASES.md` | Markdown | Test cases (earlier version) |
| `data/files (1)/CLAUDE_CODE_PROMPT.md` | Markdown | AI coding prompt used during development |
| `data/files (1)/Dockerfile` | Docker | Earlier Dockerfile |
| `data/files (1)/cdss_query_templates.py` | Python | Earlier query templates |
| `data/files (1)/cdss_rag_design.md` | Markdown | RAG design document |
| `data/files (1)/cdss_unified_schema.json` | JSON | Unified schema reference |
| `data/files (1)/chunk_for_rag.py` | Python | Earlier RAG chunking script |
| `data/files (1)/chunks_sample.jsonl` | JSONL | Sample RAG chunks |
| `data/files (1)/docker-compose.yml` | Docker Compose | Earlier compose file |
| `data/files (1)/fastapi_main.py` | Python | Earlier FastAPI main |
| `data/files (1)/indian_brand_mapper.py` | Python | Earlier Indian brand mapper |
| `data/files (1)/neo4j_populate.py` | Python | Earlier Neo4j population script |
| `data/files (1)/neo4j_schema.cypher` | Cypher | Earlier Neo4j schema |
| `data/files (1)/pass2_extractors.py` | Python | Earlier Pass 2 extractors |
| `data/files (1)/postgres_schema.sql` | SQL | Earlier PostgreSQL schema |
| `data/files (1)/postgres_schema_patch.sql` | SQL | Earlier schema patch |
| `data/files (1)/pydantic_schemas.py` | Python | Earlier Pydantic models |
| `data/files (1)/setup_cdss.sh` | Shell | CDSS setup shell script |
| `data/files (1)/setup_project.sh` | Shell | Project setup shell script |
| `data/files (1)/transform_to_unified.py` | Python | Earlier transform script |
| `data/files (1)/unified_sample.json` | JSON | Sample unified record |
| `data/files (1)/unified_sample_enriched.json` | JSON | Sample enriched unified record |

### Data Files — Samples (`data/samples/`)

| File | Type | Purpose |
|------|------|---------|
| `data/samples/chunks_sample.jsonl` | JSONL | Sample RAG chunks for testing |
| `data/samples/sample_raw_input.json` | JSON | Sample raw input record |
| `data/samples/unified_sample.json` | JSON | Sample unified output record |
| `data/samples/unified_sample_enriched.json` | JSON | Sample enriched unified output |

### Log Files (`logs/`)

| File | Table | Script that created it | Date | Status |
|------|-------|----------------------|------|--------|
| `logs/populate_drug_table.log` | `drugdb.drug` | `populate_drug_table.py` | 2026-05-02 | Shows multiple failed attempts (named cursor error) + final success (88,983 rows) |
| `logs/rxnorm_update.log` | `drugdb.drug` | `update_drug_rxnorm_columns.py` | 2026-05-02 | rxcui + rxnorm_generic_formulation filled |
| `logs/rxnorm_dryrun.log` | `drugdb.drug` | `update_drug_rxnorm_columns.py` | 2026-05-02 | Dry-run before actual update |
| `logs/rxnorm_fix.log` | `drugdb.drug` | `fix_rxnorm_uncleaned_rows.py` | 2026-05-02 | Fix run for uncleaned rxnorm rows |
| `logs/rxnorm_fix_dryrun.log` | `drugdb.drug` | `fix_rxnorm_uncleaned_rows.py` | 2026-05-02 | Dry-run for fix |
| `logs/drug_enrichment.log` | `drugdb.drug` | `update_drug_new_columns.py` | 2026-05-05 | Final enrichment: 88,983 rows in 738s; all columns filled |
| `logs/drug_enrichment_dryrun.log` | `drugdb.drug` | `update_drug_new_columns.py` | 2026-05-05 | Dry-run before enrichment (caught list.strip() bug) |
| `logs/synonym_population.log` | `drugdb.drug_synonym_formulation` | `populate_drug_synonym_formulation.py` | 2026-05-02 | Full population run (8,074 rxcuis → 88,983 formulation_ids) |
| `logs/synonym_dryrun.log` | `drugdb.drug_synonym_formulation` | `populate_drug_synonym_formulation.py` | 2026-05-02 | Dry-run before synonym population |
| `logs/ingredient_mapping.log` | `drugdb.drug_ingredient_mapping` | `populate_drug_ingredient_mapping.py` | 2026-05-02 | Mapping run (88,983 formulations checked; 20,034 ingredients; ~99.84% coverage) |
| `logs/drug_ingredient_mapping_second_pass.log` | `drugdb.drug_ingredient_mapping` | `second_pass_ingredient_mapping.py` | 2026-05-02 | Second pass for failed ingredient lookups |
| `logs/drug_ingredient_mapping_second_pass.json` | `drugdb.drug_ingredient_mapping` | `second_pass_ingredient_mapping.py` | 2026-05-02 | Structured results of second pass |
| `logs/indian_brand_drugbank.log` | `drugdb.indian_brand_ingredient` | `update_indian_brand_drugbank_id.py` | 2026-05-01 | Found 580,669 NULL records; bulk tier 1 matches succeeded |
| `logs/update_ingredient_rxcui_dryrun.log` | `drugdb.ingredients` | `update_ingredient_rxcui.py` | 2026-05-02 | Dry-run 1 for rxcui backfill |
| `logs/update_ingredient_rxcui_dryrun2.log` | `drugdb.ingredients` | `update_ingredient_rxcui.py` | 2026-05-02 | Dry-run 2 for rxcui backfill |
| `logs/update_ingredient_rxcui_run.log` | `drugdb.ingredients` | `update_ingredient_rxcui.py` | 2026-05-02 | Actual run: 2,137 unique ingredients extracted; rxcui backfilled |
| `logs/rxcui_update.log` | `drugdb.ingredients` | [INFERRED] rxcui update utility | — | Additional rxcui update tracking |
| `logs/fuzzy_match_update.log` | `drugdb.drug_ingredient_mapping` | `apply_fuzzy_matches.py` | 2026-05-01 | Found 285 unmapped ingredients; 5 updated after fuzzy dedup |
| `logs/fuzzy_ingredient_match_results.json` | `drugdb.drug_ingredient_mapping` | `fuzzy_ingredient_dedup.py` | 2026-05-01 | Fuzzy match result details |
| `logs/fuzzy_ingredient_match_apply_log.txt` | `drugdb.drug_ingredient_mapping` | `apply_fuzzy_matches.py` | 2026-05-01 | Application log for fuzzy matches |
| `logs/api.log` | API runtime | FastAPI/uvicorn | — | Runtime API request/response log |
| `logs/drug_identifier_populate_20260505_082038.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 1 of 11 (debugging) |
| `logs/drug_identifier_populate_20260505_082148.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 2 |
| `logs/drug_identifier_populate_20260505_082250.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 3 |
| `logs/drug_identifier_populate_20260505_082521.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 4 |
| `logs/drug_identifier_populate_20260505_082604.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 5 |
| `logs/drug_identifier_populate_20260505_082636.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 6 |
| `logs/drug_identifier_populate_20260505_082821.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 7 |
| `logs/drug_identifier_populate_20260505_082839.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 8 |
| `logs/drug_identifier_populate_20260505_082854.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 9 |
| `logs/drug_identifier_populate_20260505_084430.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 10 |
| `logs/drug_identifier_populate_20260505_084552.log` | `drugdb.drug_identifier` | `populate_drug_identifier.py` | 2026-05-05 | Run 11 — final successful run (578,635 rows) |
| `logs/SELECT * FROM "DrugMasterLinkage" LIMIT .pgsql` | DrugMasterLinkage | Manual psql query | — | SQL query file from interactive psql session |

### Report Files (`reports/`)

| File | Type | Purpose |
|------|------|---------|
| `reports/CDSS_DATABASE_DEEP_RESEARCH_REPORT.md` | Markdown | Comprehensive table-by-table statistics and coverage analysis (2026-05-04) |
| `reports/rxnorm_columns_population_report.md` | Markdown | Report on rxnorm column population status and coverage |
| `reports/_raw_data.json` | JSON | Raw data behind the deep research report |

### Documentation Files

| File | Type | Purpose |
|------|------|---------|
| `PIPELINE.md` | Markdown | End-to-end pipeline: 5 phases, commands, timing, verification queries |
| `FILES.md` | Markdown | Complete file inventory: all objects created by the pipeline |
| `tablecreation.md` | Markdown | Brief drug table creation command notes |
| `docs/00_INDEX.md` | Markdown | Master index linking to all 4 documentation sections |
| `docs/01_PRD_OVERVIEW.md` | Markdown | Full PRD: 9 queries, stack, 8 phases, LLM recommendations |
| `docs/02_REST_API_DESIGN.md` | Markdown | REST API endpoint design (Q1–Q9) |
| `docs/03_PROMPT_TEMPLATES.md` | Markdown | LLM prompt templates for each query type |
| `docs/04_TEST_CASES.md` | Markdown | Test cases for all 9 query endpoints |
| `docs/DRUG_DATABASE_SCHEMA_DOCUMENTATION.md` | Markdown | Detailed schema documentation with JSON paths and indexes |
| `docs/cdss_rag_design.md` | Markdown | RAG architecture design |
| `docs/cdss_unified_schema.json` | JSON | Target unified schema specification |
| `scripts/DRUG_DATABASE_SCHEMA_DOCUMENTATION.md` | Markdown | Copy of schema documentation in scripts dir |
| `data/01_PRD_OVERVIEW.md` | Markdown | Copy of PRD in data dir |
| `data/01_PRD_OVERVIEW (1).md` | Markdown | Alternate version of PRD |

### Miscellaneous Root Files

| File | Type | Purpose |
|------|------|---------|
| `phase1_analysis_report.txt` | Text | Analysis report from Phase 1 design |
| `comparison_report.txt` | Text | Comparison of old vs new dosage form values |
| `verification_report.txt` | Text | Schema verification results |
| `standardization_errors.log` | Log | Errors from standardization runs |
| `export_run.log` | Log | DrugMasterLinkage export run log |
| `stage1_execution_log.txt` | Text | Stage 1 update orchestration log |
| `fuzzy_match_results.json` | JSON | Fuzzy matching results |
| `match_statistics.json` | JSON | Matching statistics summary |
| `matched_ingredients.txt` | Text | List of matched ingredients |
| `unique_dosage_forms_in_drug_table.csv` | CSV | All unique dosage_forms values in drugdb.drug |
| `unique_specific_dosage_forms.csv` | CSV | Unique specific_dosage_form values from DrugMasterLinkage |
| `dosage_form_mappings.csv` | CSV | EU form codes → RxNorm suffix mappings |
| `dosage_form_mappings.json` | JSON | Same in JSON format |
| `dosage_form_mismatches.csv` | CSV | Forms that didn't match any suffix |
| `dosage_form_regex_patterns.json` | JSON | Compiled regex patterns for suffix stripping |
| `generic_formulation_ending_patterns.csv` | CSV | Patterns at end of generic_formulation strings |
| `durg_master_linkage_20260502_130246.txt` | Text | DrugMasterLinkage export (2026-05-02 13:02) |
| `durg_master_linkage_20260502_133158.txt` | Text | DrugMasterLinkage export (2026-05-02 13:31) |
| `durg_master_linkage_20260502_133209.txt` | Text | DrugMasterLinkage export (2026-05-02 13:32) |

---

## Infrastructure & Configuration

### Docker Compose (`docker-compose.yml`)

8 services:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `postgres` | postgres:16 + pgvector | 5432 | Main database (drug tables) |
| `neo4j` | neo4j:5.x | 7474/7687 | Graph database for drug pathways |
| `embedding` | ghcr.io/huggingface/text-embeddings-inference | 8081 | BAAI/bge-large-en-v1.5 (1024-dim) |
| `composer` | vllm/vllm-openai | 8082 | Qwen2.5-7B-Instruct (always-on, query-time formatting) |
| `extractor` | vllm/vllm-openai | 8083 | Qwen2.5-72B-Instruct-AWQ (batch-only, Phase 6) — *commented out* |
| `api` | (built from Dockerfile) | 8080 | FastAPI application |

### Dockerfile

- Base: `python:3.11-slim`
- App: FastAPI with 4-worker uvicorn
- Copies only `app/` directory

### Python Requirements (`requirements.txt`)

Key packages:
- `fastapi`, `uvicorn[standard]` — REST API
- `asyncpg`, `psycopg2-binary` — PostgreSQL async + sync drivers
- `neo4j` — Neo4j Python driver
- `tqdm` — Progress bars for long-running scripts
- `openai` — OpenAI-compatible API client (for vLLM)
- `httpx` — Async HTTP for TEI embedding calls

### Environment Configuration (`.env`)

Database credentials and service URLs. Referenced by all Python scripts.

---

## FastAPI Application Layer

### App Entry Point (`app/main.py`)

- Manages PostgreSQL connection pool (asyncpg) + Neo4j pool lifespans
- CORS middleware
- Health check endpoint (`GET /health`)
- Mounts Q1–Q9 endpoint routers

### Request/Response Models (`app/models/schemas.py`)

Complete Pydantic models for all 9 query types. Key patterns:
- `DrugIdentifier` — input: drug name/rxcui/ndc/brandname
- `PatientContext` — weight, age, renal/hepatic function
- `InteractionResult` — severity ENUM: `contraindicated | major | moderate | minor`

### Core Services (all currently stub/empty files)

| File | Purpose |
|------|---------|
| `app/core/database.py` | PostgreSQL query layer (STUB) |
| `app/core/entity_resolver.py` | Drug name → formulation_id resolution (STUB) |
| `app/core/vector_search.py` | pgvector similarity search (STUB) |
| `app/core/indian_brand_service.py` | Indian brand lookup (STUB) |
| `app/core/llm_client.py` | vLLM API client |
| `app/core/embedding_client.py` | TEI embedding client |
| `app/core/post_checks.py` | Post-processing safety checks |
| `app/core/response_composer.py` | LLM response composition |

### API Endpoints (all currently stub/empty files)

| Endpoint | File | Query |
|----------|------|-------|
| `POST /query/disorder-medications` | `q1_disorder_to_meds.py` | Q1: Which drugs treat disorder X? |
| `POST /query/drug-interactions` | `q2_interaction_check.py` | Q2: Do these drugs interact? |
| `POST /query/alternatives` | `q3_alternatives.py` | Q3: Alternatives to drug X? |
| `POST /query/dose-recommendation` | `q4_dose_recommendation.py` | Q4: Dosage for this patient? |
| `POST /query/population-approval` | `q5_population_approval.py` | Q5: Approved for population Y? |
| `POST /query/safe-drugs` | `q6_safe_drugs_for_condition.py` | Q6: Safe drugs given comorbidities? |
| `POST /query/organ-impairment-dosing` | `q7_organ_impairment_dosing.py` | Q8: Dose adjustment for organ impairment? |
| `POST /query/administration-timing` | `q8_administration_timing.py` | Q8: Food/timing requirements? |
| `POST /query/pill-burden` | `q9_pill_burden.py` | Q9: Minimum pill burden strength? |

### Test Suite (`tests/`)

| File | Tests |
|------|-------|
| `tests/conftest.py` | Fixtures: test DB connection, mock data |
| `tests/test_entity_resolver.py` | Entity resolution unit tests |
| `tests/test_normalizers.py` | Input normalization tests |
| `tests/test_q1.py` | Q1 endpoint tests |
| `tests/test_q2.py` | Q2 endpoint tests |
| `tests/test_q3.py` | Q3 endpoint tests |
| `tests/test_q4.py` | Q4 endpoint tests |
| `tests/test_q5.py` | Q5 endpoint tests |
| `tests/test_q6.py` | Q6 endpoint tests |
| `tests/test_q7.py` | Q7 endpoint tests |
| `tests/test_q8.py` | Q8 endpoint tests |
| `tests/test_q9.py` | Q9 endpoint tests |

---

## Neo4j Graph Schema (`schemas/neo4j_schema.cypher`)

**7 Node Types:**

| Node | Key Properties | Uniqueness Constraint |
|------|---------------|----------------------|
| `:Drug` | formulation_id, generic_name, drug_class[], routes[] | formulation_id |
| `:Ingredient` | name, unii, drugbank_id, role | name |
| `:Enzyme` | name, category (CYP450) | name |
| `:Target` | name, class (receptor/kinase) | name |
| `:DrugClass` | name (ATC) | name |
| `:Indication` | icd10, snomed_code | icd10 |
| `:IndianBrand` | brand_name, manufacturer_india, generic_name_normalized | (brand_name, manufacturer_india) |

**9 Relationship Types:**

| Relationship | From → To | Key Properties |
|-------------|----------|---------------|
| `CONTAINS_ACTIVE` | Drug → Ingredient | strength_label |
| `CONTAINS_EXCIPIENT` | Drug → Ingredient | — |
| `INHIBITS` | Ingredient → Enzyme | mechanism |
| `INDUCES` | Ingredient → Enzyme | — |
| `TARGETS` | Ingredient → Target | — |
| `IN_CLASS` | Drug → DrugClass | — |
| `INDICATED_FOR` | Drug → Indication | — |
| `INTERACTS_WITH` | Ingredient → Ingredient | severity (bidirectional) |
| `MAPS_TO` | IndianBrand → Drug | match_confidence |

---

## Entity Resolver Logic (`schemas/postgres_schema.sql`)

The `resolve_drug(input_name TEXT)` PostgreSQL function implements a 7-tier matching cascade:

| Tier | Strategy | Match Type |
|------|---------|-----------|
| 1 | Exact Indian brand name match | `indian_brand` |
| 2 | FDC ingredient decomposition | `fdc_ingredient` |
| 3 | FDA generic name exact match | `exact_generic` |
| 4 | Normalized generic (salt-stripped) | `normalized_generic` |
| 5 | DrugBank synonym match (via `ingredient_synonym`) | `drugbank_synonym` |
| 6 | Fuzzy Indian brand (trigram similarity > 0.6) | `fuzzy_indian` |
| 7 | Fuzzy generic (trigram similarity > 0.5) | `fuzzy_generic` |

---

## Current Status Summary (as of 2026-05-05)

| Table | Rows | Status |
|-------|------|--------|
| `DrugSourceMaster` | 738,197 | ✅ Complete (input) |
| `DrugSourceMaster.standardized_records` | 738,197 | ✅ Complete |
| `drugdb.ingredients` | 20,037 | ✅ Complete (99.1% drugbank_id; 10.7% rxcui) |
| `drugdb.ingredient_synonyms` | 52,154 | ✅ Complete |
| `drugdb.ingredient_interactions` | 2,910,556 | ✅ Complete (600 dropped) |
| `drugdb.drug` | 88,983 | ✅ Complete (all 16 columns filled) |
| `drugdb.drug_identifier` | 578,635 | ✅ Complete |
| `drugdb.drug_synonym_formulation` | ~66,000 | ✅ Complete (74.34% coverage) |
| `drugdb.drug_ingredient_mapping` | 98,832 | ✅ Complete (99.84% coverage) |
| `drugdb.indian_brand_ingredient` | 580,669 | ⚠️ Partial (drugbank_id being filled) |
| `public.drug_indication` | 0 | ⏳ Pending Phase 6 (LLM) |
| `public.drug_interaction` | 0 | ⏳ Pending Phase 6 (LLM) |
| `public.dosing_regimen` | 0 | ⏳ Pending Phase 6 (LLM) |
| `public.population_approval` | 0 | ⏳ Pending Phase 6 (LLM) |
| `public.administration_timing` | 0 | ⏳ Pending Phase 6 (LLM) |
| `public.active_ingredient` | 0 | ⏳ Pending Phase 6 (LLM) |
| `public.inactive_ingredient` | 0 | ⏳ Pending Phase 6 (LLM) |
| `public.clinical_section` | 0 | ⏳ Pending Phase 6 (LLM) |
| `public.rag_chunk` | 0 | ⏳ Pending Phase 7 (embedding) |
| `public.indian_brand` | 0 | ⏳ Pending Phase 9 (Indian brands) |
| `public.query_audit_log` | 0 | ⏳ Populated at runtime |
| **Neo4j graph** | 0 nodes | ⏳ Pending Phase 8 |
| **FastAPI endpoints** | — | ⏳ Stub (schema + models done) |
