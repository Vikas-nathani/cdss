# 🔬 Drug Data Processing Pipeline

Complete documentation of the drug data ingestion and standardization workflow.
This pipeline processed **738,197 records** from 4 sources into a unified schema.

---

## 📋 Overview

| Item | Detail |
|---|---|
| **Sources** | OpenFDA (256,165), DailyMed (51,731), DrugBank (19,842), RxNorm (410,459) |
| **Total records** | 738,197 |
| **Phases** | 6 |
| **Scripts** | 10 |
| **Database** | PostgreSQL @ 178.236.185.230, database: `postgres` |
| **Source table** | `public.DrugSourceMaster` + `public.DrugMasterLinkage` |
| **Target schema** | `drugdb` |

---

## 🗺️ Visual Pipeline

```
public.DrugSourceMaster
├── source='openfda'   (256,165 rows)  clean_record (JSONB)
├── source='dailymed'  ( 51,731 rows)  clean_record (JSONB)
├── source='drugbank'  ( 19,842 rows)  clean_record (JSONB)
└── source='rxnorm'    (410,459 rows)  record       (JSONB)
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 1: Schema Extraction                                     │
│                                                                 │
│  extract_schema_openfda.py  → master_schema_openfda.json        │
│  extract_schema_dailymed.py → master_schema_dailymed.json       │
│  extract_drugbank_schema.py → master_schema_drugbank.json       │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 2: Schema Normalization                                  │
│                                                                 │
│  normalize_openfda.py  → normalized_schema.json (13 categories) │
│  normalize_dailymed.py → master_schema_dailymed_normalized.json │
│  normalize_drugbank.py → master_schema_drugbank_normalized.json │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 3: Data Standardization                                  │
│                                                                 │
│  populate_standardized.py  → OpenFDA + DailyMed rows           │
│  populate_remaining.py     → DrugBank + RxNorm rows            │
│           ↓                                                     │
│  DrugSourceMaster.standardized_records  (738,197 rows filled)  │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 4: Target Schema Creation                                │
│                                                                 │
│  ingredient_schema.sql (DDL in postgres DB, drugdb schema)      │
│           ↓                                                     │
│  drugdb.ingredients          (table)                           │
│  drugdb.ingredient_synonyms  (table)                           │
│  drugdb.ingredient_interactions    (table)                           │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 5: Database Population                                   │
│                                                                 │
│  drugdb_migration.sql  (WHERE source='drugbank')               │
│           ↓                                                     │
│  drugdb.ingredients          →  19,842 rows                    │
│  drugdb.ingredient_synonyms  →  52,154 rows                    │
│  drugdb.ingredient_interactions    → 2,910,556 rows            │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 6: Label Table Population                                │
│                                                                 │
│  populate_label_table.py                                        │
│    Source: DrugMasterLinkage.combined_clean_jsonb               │
│    Lookup: drugdb.drug  (master_linkage_id → formulation_ids)   │
│           ↓                                                     │
│  drugdb.label_table          →  510,527 rows                   │
│    50,111 linkage records processed                             │
│    88,983 formulation IDs matched                               │
│     2,492 skipped (no formulation match — data gap)            │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 7: Clinical Section Population                           │
│                                                                 │
│  populate_clinical_section.py                                   │
│    Source: DrugMasterLinkage.combined_clean_jsonb               │
│    Lookup: drugdb.drug  (master_linkage_id → formulation_ids)   │
│           ↓                                                     │
│  drugdb.clinical_section     →  2,887,910 rows                 │
│    50,111 linkage records processed                             │
│    88,983 formulation IDs matched                               │
│    45 unique sections discovered (dynamically)                  │
│    openfda: 1,840,508 rows  |  dailymed: 1,047,402 rows        │
│     2,492 skipped (no formulation match — data gap)            │
└─────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ Prerequisites

### Database Access
```
Host     : 178.236.185.230
Port     : 5432
Database : postgres
User     : postgres
Password : ********
```

### Python Libraries
```bash
pip install psycopg2-binary tqdm
```

### Working Directory
All Python scripts must be run from:
```bash
cd /home/nathanivikas890_gmail_com/cdss
```

---

## 📦 Phase 1: Schema Extraction

**Purpose:** Discover the complete set of unique field paths that exist across all records for each source. The output is a "master template" JSON where every key that ever appeared in any record is present, with null leaves.

---

### Step 1.1 — OpenFDA Schema Extraction

**Script:** `scripts/extract_schema_openfda.py`

**Command:**
```bash
python scripts/extract_schema_openfda.py
```

**Input:**
| Item | Value |
|---|---|
| Table | `public.DrugSourceMaster` |
| Filter | `WHERE source = 'openfda'` |
| Column | `clean_record` (JSONB) |
| Row count | 256,165 |

**Output:**
| File | Description |
|---|---|
| `data/master_schema_openfda.json` | 163 unique top-level field paths |

**Verification:**
```bash
# File exists and has content
ls -lh data/master_schema_openfda.json

# Count top-level fields
python3 -c "import json; d=json.load(open('data/master_schema_openfda.json')); print(len(d), 'fields')"
```
✅ **Expected:** 163 top-level fields

---

### Step 1.2 — DailyMed Schema Extraction

**Script:** `scripts/extract_schema_dailymed.py`

**Command:**
```bash
python scripts/extract_schema_dailymed.py
```

**Input:**
| Item | Value |
|---|---|
| Table | `public.DrugSourceMaster` |
| Filter | `WHERE source = 'dailymed'` |
| Column | `clean_record` (JSONB) |
| Row count | 51,731 |

**Output:**
| File | Description |
|---|---|
| `data/master_schema_dailymed.json` | Structural keys: `products`, `drug_label`, `manufacturer`, `label_sections` |

**Verification:**
```bash
ls -lh data/master_schema_dailymed.json
python3 -c "import json; d=json.load(open('data/master_schema_dailymed.json')); print(list(d.keys()))"
```
✅ **Expected:** `['products', 'drug_label', 'manufacturer', 'label_sections']`

---

### Step 1.3 — DrugBank Schema Extraction

**Script:** `scripts/extract_drugbank_schema.py`

**Command:**
```bash
python scripts/extract_drugbank_schema.py
```

**Input:**
| Item | Value |
|---|---|
| Table | `public.DrugSourceMaster` |
| Filter | `WHERE source = 'drugbank'` |
| Column | `clean_record` (JSONB) |
| Row count | 19,842 |

**Output:**
| File | Description |
|---|---|
| `data/master_schema_drugbank.json` | 11 top-level fields |

**Verification:**
```bash
ls -lh data/master_schema_drugbank.json
python3 -c "import json; d=json.load(open('data/master_schema_drugbank.json')); print(list(d.keys()))"
```
✅ **Expected:** `['name', 'unii', 'synonyms', 'reactions', 'indication', 'drugbank_id', 'general_function', 'pharmacodynamics', 'drug_interactions', 'food_interactions', 'classification_description']`

---

## 🗂️ Phase 2: Schema Normalization

**Purpose:** Reorganize the raw flat schema into meaningful semantic categories so all downstream code can work with a consistent, human-readable structure regardless of source.

---

### Step 2.1 — OpenFDA Normalization

**Script:** `scripts/normalize_openfda.py`

**Command:**
```bash
python scripts/normalize_openfda.py
```

**Input:**
| File | Description |
|---|---|
| `data/master_schema_openfda.json` | 163 flat field paths |

**Output:**
| File | Categories |
|---|---|
| `data/normalized_schema.json` | `drug_info`, `identification`, `labeling_content`, `safety`, `adverse_events`, `clinical`, `patient_info`, `population_specific`, `drug_interactions`, `supply_storage`, `abuse_dependence`, `device`, `openfda_metadata` |

**Verification:**
```bash
python3 -c "import json; d=json.load(open('data/normalized_schema.json')); print(list(d.keys()))"
```
✅ **Expected:** 13 semantic category keys

---

### Step 2.2 — DailyMed Normalization

**Script:** `scripts/normalize_dailymed.py`

**Command:**
```bash
python scripts/normalize_dailymed.py
```

**Input:**
| File | Description |
|---|---|
| `data/master_schema_dailymed.json` | Raw DailyMed structure |

**Output:**
| File | Categories |
|---|---|
| `data/master_schema_dailymed_normalized.json` | `drug_info`, `identification`, `labeling_content`, `safety`, `adverse_events`, `clinical`, `patient_info`, `population_specific`, `drug_interactions`, `supply_storage` |

**Verification:**
```bash
python3 -c "import json; d=json.load(open('data/master_schema_dailymed_normalized.json')); print(list(d.keys()))"
```
✅ **Expected:** 10 semantic category keys

---

### Step 2.3 — DrugBank Normalization

**Script:** `scripts/normalize_drugbank.py`

**Command:**
```bash
python scripts/normalize_drugbank.py
```

**Input:**
| File | Description |
|---|---|
| `data/master_schema_drugbank.json` | 11 flat DrugBank fields |

**Output:**
| File | Categories |
|---|---|
| `data/master_schema_drugbank_normalized.json` | `drug_info`, `clinical`, `drug_interactions`, `chemistry` |

**Verification:**
```bash
python3 -c "import json; d=json.load(open('data/master_schema_drugbank_normalized.json')); print(list(d.keys()))"
```
✅ **Expected:** `['drug_info', 'clinical', 'drug_interactions', 'chemistry']`

---

## 🔄 Phase 3: Data Standardization

**Purpose:** Apply the normalized schemas to transform every raw `clean_record` into the unified `standardized_records` format. This phase ran in two scripts — the second picked up after the first completed OpenFDA and DailyMed.

---

### Step 3.1 — OpenFDA + DailyMed Population

**Script:** `scripts/populate_standardized.py`

**Command:**
```bash
python scripts/populate_standardized.py
```

**Input:**
| Item | Value |
|---|---|
| Sources processed | `openfda`, `dailymed` |
| Source column | `clean_record` |
| Schema files | `normalized_schema.json`, `master_schema_dailymed_normalized.json` |

**Output:** `DrugSourceMaster.standardized_records` populated for 307,896 rows (openfda + dailymed)

**Verification:**
```sql
SELECT source, COUNT(*) FILTER (WHERE standardized_records IS NOT NULL) AS populated
FROM public."DrugSourceMaster"
WHERE source IN ('openfda', 'dailymed')
GROUP BY source;
```
✅ **Expected:** openfda=256,165 | dailymed=51,731

---

### Step 3.2 — DrugBank + RxNorm Population

**Script:** `scripts/populate_remaining.py`

**Command:**
```bash
python scripts/populate_remaining.py
```

**Input:**
| Item | Value |
|---|---|
| Sources processed | `drugbank`, `rxnorm` |
| DrugBank column | `clean_record` — transformed via `master_schema_drugbank_normalized.json` |
| RxNorm column | `record` — copied as-is (no transformation) |

**Output:** `DrugSourceMaster.standardized_records` populated for 430,301 rows (drugbank + rxnorm)

**Verification:**
```sql
SELECT source, COUNT(*) FILTER (WHERE standardized_records IS NOT NULL) AS populated
FROM public."DrugSourceMaster"
WHERE source IN ('drugbank', 'rxnorm')
GROUP BY source;
```
✅ **Expected:** drugbank=19,842 | rxnorm=410,459

---

## 🏗️ Phase 4: Target Schema Creation

**Purpose:** Create the three target tables in the `drugdb` schema that will hold the structured ingredient and interaction data extracted from DrugBank.

---

### Step 4.1 — Create drugdb Tables

**Script:** `ingredient_schema.sql` (DDL run directly in postgres DB)

**Command:**
```bash
# Schema and tables already exist in the postgres database
# To inspect:
psql -h 178.236.185.230 -U postgres -d postgres -c "\dt drugdb.*"
```

**Output:**
| Table | Description |
|---|---|
| `drugdb.ingredients` | One row per DrugBank ingredient |
| `drugdb.ingredient_synonyms` | One row per synonym per ingredient |
| `drugdb.ingredient_interactions` | One row per directed interaction pair |

**Verification:**
```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'drugdb'
ORDER BY table_name;
```
✅ **Expected:** `ingredient_interactions`, `ingredient_synonyms`, `ingredients`

---

## 📥 Phase 5: Database Population

**Purpose:** Extract structured data from `DrugSourceMaster.standardized_records` (DrugBank source only) and populate the three `drugdb` tables.

---

### Step 5.1 — Populate drugdb Tables

**Script:** `schemas/drugdb_migration.sql`

**Command:**
```bash
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/drugdb_migration.sql
```

**Input:**
| Item | Value |
|---|---|
| Source table | `public.DrugSourceMaster` |
| Filter | `WHERE source = 'drugbank'` |
| Source column | `standardized_records` (JSONB) |
| Rows processed | 19,842 |

**JSON paths used:**

| Target column | Source path |
|---|---|
| `ingredients.drugbank_id` | `standardized_records->'drug_info'->>'drugbank_id'` |
| `ingredients.name` | `standardized_records->'drug_info'->>'name'` |
| `ingredients.unii` | `standardized_records->'drug_info'->>'unii'` |
| `ingredients.indications` | `standardized_records->'clinical'->>'indication'` |
| `ingredients.general_function` | `standardized_records->'clinical'->>'general_function'` |
| `ingredients.pharmacodynamics` | `standardized_records->'clinical'->>'pharmacodynamics'` |
| `ingredients.classification_description` | `standardized_records->'drug_info'->>'classification_description'` |
| `ingredients.food_interactions` | `standardized_records->'drug_interactions'->'food_interactions'` (as JSON text) |
| `ingredient_synonyms.synonym` | `standardized_records->'drug_info'->'synonyms'[]` (one row per element) |
| `ingredient_interactions.reacting_id` | Resolved via `DrugSourceMaster.sourceid` lookup |
| `ingredient_interactions.description` | `standardized_records->'drug_interactions'->'drug_interactions'[i]->>'description'` |

**Output:**

| Table | Rows inserted |
|---|---|
| `drugdb.ingredients` | **19,842** |
| `drugdb.ingredient_synonyms` | **52,154** |
| `drugdb.ingredient_interactions` | **2,910,556** |

> ⚠️ **Note:** 600 interaction rows were intentionally skipped. They referenced two DrugBank IDs (`DB09368` — 542 refs, `DB24348` — 58 refs) that have no full record in DrugSourceMaster. These are known gaps in the source data.

**Verification:**
```sql
SELECT 'ingredients'        AS tbl, COUNT(*) FROM drugdb.ingredients
UNION ALL
SELECT 'ingredient_synonyms',        COUNT(*) FROM drugdb.ingredient_synonyms
UNION ALL
SELECT 'ingredient_interactions',    COUNT(*) FROM drugdb.ingredient_interactions;
```
✅ **Expected:** 19,842 | 52,154 | 2,910,556

---

## 🏷️ Phase 6: Label Table Population

**Purpose:** Extract all structured tables embedded in drug label JSON (`combined_clean_jsonb`) from `DrugMasterLinkage` and store them as structured rows in `drugdb.label_table`. Supports one row per table per formulation — each `master_linkage_id` maps to multiple `formulation_id` rows.

---

### Step 6.1 — Populate drugdb.label_table

**Script:** `populate_label_table.py`

**Command:**
```bash
python populate_label_table.py
```

**Input:**

| Item | Value |
|---|---|
| Source table | `public."DrugMasterLinkage"` |
| Source column | `combined_clean_jsonb` (JSONB) |
| Sections checked | All child sections under: `safety`, `adverse_events`, `labeling_content`, `clinical`, `drug_interactions`, `population_specific` |
| Lookup table | `drugdb.drug` (master_linkage_id → formulation_ids) |

**Output:**

| Table | Rows inserted |
|---|---|
| `drugdb.label_table` | **510,527** |

**Output columns:**

| Column | Type | Description |
|---|---|---|
| `formulation_id` | UUID | Links to `drugdb.drug.formulation_id` |
| `table_id` | TEXT | `{formulation_id}_{section_key}_table_{N}` |
| `caption` | TEXT | Table caption from label JSON (NULL if absent) |
| `semantic_type` | TEXT | Mapped type: `adverse_event`, `dosing`, `interaction`, `pharmacokinetics`, `clinical_study`, `contraindication`, or NULL |
| `section` | TEXT | Source section key (e.g. `adverse_reactions`, `clinical_studies`) |
| `headers` | TEXT[] | Column headers (promoted from first row if all n/% values) |
| `rows_data` | JSONB | Table rows as-is (array-of-arrays or array-of-objects) |

**Breakdown by semantic_type:**

| semantic_type | Rows |
|---|---|
| `adverse_event` | 159,368 |
| `pharmacokinetics` | 91,968 |
| `clinical_study` | 118,377 |
| `dosing` | 77,520 |
| `interaction` | 22,506 |
| `contraindication` | 140 |
| NULL (unmapped sections) | 40,648 |

**Verification:**
```sql
SELECT count(*) FROM drugdb.label_table;
-- Expected: 510,527

SELECT semantic_type, count(*) FROM drugdb.label_table
GROUP BY semantic_type ORDER BY count(*) DESC;
```

---

## ⚡ Quick Start — Run Entire Pipeline

```bash
# From project root: /home/nathanivikas890_gmail_com/cdss

# Phase 1: Schema Extraction
python scripts/extract_schema_openfda.py
python scripts/extract_schema_dailymed.py
python scripts/extract_drugbank_schema.py

# Phase 2: Schema Normalization
python scripts/normalize_openfda.py
python scripts/normalize_dailymed.py
python scripts/normalize_drugbank.py

# Phase 3: Data Standardization
python scripts/populate_standardized.py   # OpenFDA + DailyMed
python scripts/populate_remaining.py      # DrugBank + RxNorm

# Phase 4: Schema already exists — verify with:
psql -h 178.236.185.230 -U postgres -d postgres -c "\dt drugdb.*"

# Phase 5: Populate drugdb tables
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/drugdb_migration.sql

# Phase 6: Populate label_table (interactive — answer y to proceed)
python populate_label_table.py

# Verify everything
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/verification_queries.sql
```

Or run the shell script:
```bash
bash run_all.sh
```

---

## ⏱️ Estimated Timing

| Phase | Duration |
|---|---|
| Phase 1 — Schema Extraction (×3) | ~5 min total |
| Phase 2 — Normalization (×3) | ~2 min total |
| Phase 3 — Standardization (738K rows) | ~45–90 min |
| Phase 4 — Schema creation | < 1 min |
| Phase 5 — drugdb population | ~10–15 min |
| Phase 6 — label_table population (50K records) | ~15 min |
| Phase 7 — clinical_section population (50K records) | ~57 min |
| **Total** | **~2.5–3.5 hours** |
