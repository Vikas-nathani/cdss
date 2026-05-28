# 📁 File Inventory — Drug Data Pipeline

All files produced by or used in the drug data processing pipeline.

---

## 🗄️ Database Objects

### Source (input — pre-existing)

| Object | Type | Location | Description |
|---|---|---|---|
| `DrugSourceMaster` | Table | `public` schema, `postgres` DB | 738,197 raw drug records from 4 sources |
| `DrugSourceMaster.clean_record` | Column (JSONB) | — | Pre-processed raw record for openfda, dailymed, drugbank |
| `DrugSourceMaster.record` | Column (JSONB) | — | Raw record for rxnorm (no clean_record equivalent) |
| `DrugSourceMaster.source` | Column (TEXT) | — | Source identifier: openfda, dailymed, drugbank, rxnorm |
| `DrugSourceMaster.sourceid` | Column (TEXT) | — | Source-native ID (e.g. DrugBank ID like DB00001) |

### Created by this pipeline

| Object | Type | Location | Rows | Created in Phase |
|---|---|---|---|---|
| `DrugSourceMaster.standardized_records` | Column (JSONB) | `public`, `postgres` DB | 738,197 | Phase 3 |
| `drugdb.ingredients` | Table | `drugdb`, `postgres` DB | 19,842 | Phase 5 |
| `drugdb.ingredient_synonyms` | Table | `drugdb`, `postgres` DB | 52,154 | Phase 5 |
| `drugdb.ingredient_interactions` | Table | `drugdb`, `postgres` DB | 2,910,556 | Phase 5 |
| `drugdb.label_table` | Table | `drugdb`, `postgres` DB | 510,527 | Phase 6 |
| `drugdb.clinical_section` | Table | `drugdb`, `postgres` DB | 2,887,910 | Phase 7 |

---

## 🐍 Python Scripts

### Phase 1 — Schema Extraction

| File | Input | Output | Phase |
|---|---|---|---|
| `scripts/extract_schema_openfda.py` | `DrugSourceMaster.clean_record` WHERE source='openfda' | `data/master_schema_openfda.json` | 1 |
| `scripts/extract_schema_dailymed.py` | `DrugSourceMaster.clean_record` WHERE source='dailymed' | `data/master_schema_dailymed.json` | 1 |
| `scripts/extract_drugbank_schema.py` | `DrugSourceMaster.clean_record` WHERE source='drugbank' | `data/master_schema_drugbank.json` | 1 |

### Phase 2 — Schema Normalization

| File | Input | Output | Phase |
|---|---|---|---|
| `scripts/normalize_openfda.py` | `data/master_schema_openfda.json` | `data/normalized_schema.json` | 2 |
| `scripts/normalize_dailymed.py` | `data/master_schema_dailymed.json` | `data/master_schema_dailymed_normalized.json` | 2 |
| `scripts/normalize_drugbank.py` | `data/master_schema_drugbank.json` | `data/master_schema_drugbank_normalized.json` | 2 |

### Phase 6 — Label Table Population

| File | Input | Output | Phase |
|---|---|---|---|
| `populate_label_table.py` | `public."DrugMasterLinkage".combined_clean_jsonb` + `drugdb.drug` | `drugdb.label_table` (510,527 rows) | 6 |

### Phase 7 — Clinical Section Population

| File | Input | Output | Phase |
|---|---|---|---|
| `populate_clinical_section.py` | `public."DrugMasterLinkage".combined_clean_jsonb` + `drugdb.drug` | `drugdb.clinical_section` (2,887,910 rows) | 7 |

---

## 📋 Run Reports

| File | Description |
|---|---|
| `label_table_population.log` | Full run log: batches, warnings, timing, final summary |
| `clinical_section_population.log` | Full run log for clinical_section population: batches, warnings, timing, final summary |
| `reports/label_table_population_report.md` | Concise summary report for the label_table population run |
| `reports/clinical_section_population_report.md` | Concise summary report for the clinical_section population run |
| `reports/rxnorm_columns_population_report.md` | Summary report for rxnorm column population run |

---

### Phase 3 — Data Standardization

| File | Input | Output | Sources handled | Phase |
|---|---|---|---|---|
| `scripts/populate_standardized.py` | `clean_record` + normalized schema JSONs | `standardized_records` column | openfda, dailymed | 3 |
| `scripts/populate_remaining.py` | `clean_record` / `record` + normalized schema JSONs | `standardized_records` column | drugbank, rxnorm | 3 |

---

## 🗃️ SQL Scripts

| File | Purpose | Phase |
|---|---|---|
| `schemas/drugdb_migration.sql` | Populates `drugdb.ingredients`, `drugdb.ingredient_synonyms`, `drugdb.ingredient_interactions` from `standardized_records` | 5 |
| `schemas/verification_queries.sql` | SQL queries to verify all 5 phases completed correctly | All |

---

## 📊 JSON Schema Files

### Phase 1 outputs — Raw extracted schemas

| File | Source | Top-level keys | Description |
|---|---|---|---|
| `data/master_schema_openfda.json` | OpenFDA | 163 | Union of all field paths seen across 256,165 OpenFDA records |
| `data/master_schema_dailymed.json` | DailyMed | 4 (`products`, `drug_label`, `manufacturer`, `label_sections`) | Structural union of DailyMed records |
| `data/master_schema_drugbank.json` | DrugBank | 11 | Union of all field paths across 19,842 DrugBank records |

### Phase 2 outputs — Normalized schemas

| File | Source | Categories | Description |
|---|---|---|---|
| `data/normalized_schema.json` | OpenFDA | 13 | Flat fields reorganized into semantic categories |
| `data/master_schema_dailymed_normalized.json` | DailyMed | 10 | Structural keys mapped to semantic categories |
| `data/master_schema_drugbank_normalized.json` | DrugBank | 4 (`drug_info`, `clinical`, `drug_interactions`, `chemistry`) | DrugBank fields in semantic categories |

### Semantic categories (OpenFDA/DailyMed shared)

| Category | Content |
|---|---|
| `drug_info` | Identity fields: name, ids, manufacturer, products |
| `identification` | Document-level: set_id, version, effective_date |
| `labeling_content` | Indications, dosage forms, administration instructions |
| `safety` | Warnings, contraindications, boxed warnings, precautions |
| `adverse_events` | Adverse reactions, overdosage |
| `clinical` | Pharmacology, pharmacokinetics, mechanism of action, studies |
| `patient_info` | Medication guide, instructions for patients |
| `population_specific` | Pediatric, geriatric, pregnancy, nursing use |
| `drug_interactions` | Drug-drug and lab interactions |
| `supply_storage` | How supplied, storage conditions, packaging |
| `abuse_dependence` | Abuse, dependence, controlled substance info (OpenFDA only) |
| `device` | Device-specific fields (OpenFDA only) |
| `openfda_metadata` | OpenFDA metadata block (OpenFDA only) |

---

## 📄 Other Data Files

| File | Description |
|---|---|
| `data/field_mapping_openfda.json` | Detailed mapping: raw OpenFDA field → normalized path |
| `data/field_mapping_dailymed.json` | Detailed mapping: raw DailyMed field → normalized path |
| `data/drugbank_field_mapping.json` | Detailed mapping: raw DrugBank field → normalized path |
| `data/normalization_stats.json` | Stats from normalization run (before/after/verification/categories) |
| `data/normalization_stats_drugbank.json` | DrugBank-specific normalization stats |
| `data/transformation_samples.json` | Before/after sample records for each source (10 per source) |
| `data/sample_rows.json` | First 2 raw rows from last verify_schema.py run |
| `data/sample_master_schema.json` | Schema derived from last verify_schema.py sample run |
| `data/comparison_detail.json` | Field-level comparison detail across sources |
| `data/drugbank_field_catalog.txt` | Human-readable DrugBank field catalog |
| `data/drugbank_normalization_report.txt` | DrugBank normalization run report |
| `schemas/drugdb_data_quality_report.sql` | Full data quality analysis queries with results from 2026-04-30 |

---

## 🔧 Utility Scripts (not part of the main pipeline)

| File | Purpose |
|---|---|
| `scripts/verify_schema.py` | Validates schema overlay logic against live data (read-only) |
| `scripts/phase1_analysis.py` | Analysis runner used during design of the normalization mapping |
| `scripts/standardize_drug_source.py` | Earlier version of the standardization logic (superseded by populate_standardized.py) |
| `scripts/standardize_records.py` | Earlier draft of Phase 3 population (superseded) |
| `scripts/compare_records.py` | Compares records across sources for consistency checking |
