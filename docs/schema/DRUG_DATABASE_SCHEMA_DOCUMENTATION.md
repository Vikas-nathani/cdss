# CDSS Drug Database — Schema & Pipeline Documentation

**Database:** `postgres` on host `178.236.185.230`  
**Last updated:** 2026-05-06  
**Status:** Stage 1 (drug + drug_identifier) complete. Stage 2 (label_table) complete. Stage 2b (clinical_section) complete. Pass 2 extraction tables pending.

---

## Table of Contents

1. [End-to-End Data Pipeline](#1-end-to-end-data-pipeline)
2. [Source Data Sources](#2-source-data-sources)
3. [Pipeline Stages](#3-pipeline-stages)
4. [Table Schemas — All Tables](#4-table-schemas--all-tables)
5. [Population Scripts Reference](#5-population-scripts-reference)
6. [Run History — drug_identifier](#6-run-history--drug_identifier)
7. [Verification Queries](#7-verification-queries)

---

## 1. End-to-End Data Pipeline

```
RAW SOURCES                STANDARDIZATION             UNIFIED RECORD             POSTGRES TABLES
──────────────             ───────────────             ──────────────             ───────────────

openFDA JSON        ──►  normalize_openfda.py    ──►
DailyMed XML        ──►  normalize_dailymed.py   ──►  transform_to_unified.py ──►  DrugMasterLinkage
DrugBank XML        ──►  normalize_drugbank.py   ──►    (combined_clean_jsonb)
RxNorm API          ──►  standardize_records.py  ──►

                                                                              ──►  drugdb.drug
                                                                                     ↓
                                                         populate_drug_identifier.py
                                                                              ──►  drugdb.drug_identifier

                              populate_label_table.py (Stage 2 — complete)
                                                                              ──►  drugdb.label_table ✅ 510,527 rows

                              populate_clinical_section.py (Stage 2b — complete)
                                                                              ──►  drugdb.clinical_section ✅ 2,887,910 rows

                              Pass 2 (LLM batch extraction — pending)
                                                                              ──►  public.active_ingredient
                                                                              ──►  public.inactive_ingredient
                                                                              ──►  public.drug_indication
                                                                              ──►  public.drug_interaction
                                                                              ──►  public.contraindication
                                                                              ──►  public.dosing_regimen
                                                                              ──►  public.population_approval
                                                                              ──►  public.administration_timing
                                                                              ──►  public.available_strength
                                                                              ──►  public.adverse_event
                                                                              ──►  public.warning
                                                                              ──►  public.rxnorm_formulation

                              Pass 3 (embedding — pending)
                                                                              ──►  public.rag_chunk

                              Indian brand data (pending)
                                                                              ──►  public.indian_brand
                                                                              ──►  public.indian_brand_ingredient
```

---

## 2. Source Data Sources

| Source | Format | Coverage | Key fields extracted |
|--------|--------|----------|----------------------|
| **openFDA** | JSON (label API) | ~88K products | rxcui, NDC, SPL IDs, interactions, indications, dosing sections |
| **DailyMed** | XML → JSON | ~90K products | active_ingredients, products (NDC + strength), application_number |
| **DrugBank** | XML | ~12K drugs | drugbank_id, interactions, mechanisms, ATC codes |
| **RxNorm API** | REST JSON | ~50K rxcui mapped | generic name, TTY (SCD/SBD), ingredient linkage |

All four sources are merged per drug into a single JSONB record in `public."DrugMasterLinkage".combined_clean_jsonb`. The merge key is `master_linkage_id` (UUID).

**Actual JSONB top-level keys:**
```
combined_clean_jsonb
├── openfda
│   └── openfda_metadata          ← rxcui, ndc, spl_id, spl_set_id, unii, upc, application_number
├── dailymed
│   └── drug_info
│       └── products[]            ← active_ingredients[].strength, approval_id (application_number)
├── drugbank[]
│   └── drug_info
│       └── drugbank_id
└── rxnorm[]
    └── (strength, tty, name)
```

> **Important path correction:** The original spec described `openfda_metadata` at the top level of the JSONB. The actual data nests it at `openfda.openfda_metadata`. All extraction scripts use the corrected path.

---

## 3. Pipeline Stages

### Stage 0 — Source standardization

| Script | Input | Output | Status |
|--------|-------|--------|--------|
| `scripts/normalize_openfda.py` | Raw openFDA JSON files | Normalized openFDA JSONB | Complete |
| `scripts/normalize_dailymed.py` | Raw DailyMed XML | Normalized DailyMed JSONB | Complete |
| `scripts/normalize_drugbank.py` | Raw DrugBank XML | Normalized DrugBank JSONB | Complete |
| `scripts/standardize_records.py` | Per-source normalized records | Standardized per-source records | Complete |
| `scripts/transform_to_unified.py` | All standardized sources | `DrugMasterLinkage.combined_clean_jsonb` | Complete |

### Stage 1 — Core drug table

| Script | Input | Output table | Rows | Status |
|--------|-------|--------------|------|--------|
| `scripts/populate_drug_table.py` | `DrugMasterLinkage.combined_clean_jsonb` via rxnorm[] entries | `drugdb.drug` | 88,983 | **Complete** |

Each rxnorm entry (SCD/SBD tty) in the JSONB becomes one row in `drugdb.drug`. One `master_linkage_id` can produce multiple drug formulations (one per strength/form).

### Stage 2 — Identifier lookup table

| Script | Input | Output table | Rows | Status |
|--------|-------|--------------|------|--------|
| `scripts/populate_drug_identifier.py` | Streaming JOIN of `DrugMasterLinkage` + `drugdb.drug` | `drugdb.drug_identifier` | 578,635 | **Complete** |

Full details in [Section 6](#6-run-history--drug_identifier).

### Stage 2 — Label table extraction (complete)

| Script | Input | Output table | Rows | Status |
|--------|-------|--------------|------|--------|
| `populate_label_table.py` | `DrugMasterLinkage.combined_clean_jsonb` | `drugdb.label_table` | 510,527 | **Complete** |

Extracts all structured tables from openfda label sections dynamically. One row per table per formulation_id. Sections covered: `safety`, `adverse_events`, `labeling_content`, `clinical`, `drug_interactions`, `population_specific`. Log: `label_table_population.log`.

### Stage 2b — Clinical section extraction (complete)

| Script | Input | Output table | Rows | Status |
|--------|-------|--------------|------|--------|
| `populate_clinical_section.py` | `DrugMasterLinkage.combined_clean_jsonb` | `drugdb.clinical_section` | 2,887,910 | **Complete** |

Extracts all narrative text sections from both openfda and dailymed label data. One row per section per source per formulation_id. Sections discovered dynamically across 9 parent keys — 45 unique sections found. openfda: 1,840,508 rows; dailymed: 1,047,402 rows. Log: `clinical_section_population.log`.

### Stage 3 — Pass 2 structured extraction (pending)

LLM batch extraction using Qwen2.5-72B to populate the 12 structured fact tables: `active_ingredient`, `inactive_ingredient`, `drug_indication`, `drug_interaction`, `contraindication`, `dosing_regimen`, `population_approval`, `administration_timing`, `available_strength`, `adverse_event`, `warning`, `clinical_section`, `rxnorm_formulation`.

Script: `scripts/run_pass2.py` → calls `scripts/pass2_extractors.py`

### Stage 4 — Embedding (pending)

Embeds all RAG chunks from `clinical_section` into `rag_chunk.embedding` (vector(1024), bge-large-en-v1.5).

Script: `scripts/embed_chunks.py` → `scripts/chunk_for_rag.py`

### Stage 5 — Indian brand mapping (pending)

Loads Indian brand CSV → `indian_brand` and `indian_brand_ingredient` tables, then runs fuzzy matching to link `formulation_id`.

Scripts: `scripts/indian_brand_loader.py` → `scripts/indian_brand_mapper.py`

### Stage 6 — Neo4j graph (pending)

Populates graph nodes and relationships for drug interaction pathway traversal.

Script: `scripts/neo4j_populate.py`

---

## 4. Table Schemas — All Tables

### 4.1 `drugdb.drug`

**Purpose:** One row per drug formulation (strength × dosage form combination). Primary anchor table — all other tables FK to this via `formulation_id`.

**Row count:** 88,983  
**Population script:** `scripts/populate_drug_table.py`  
**Source:** `DrugMasterLinkage.combined_clean_jsonb` via `rxnorm[]` entries

```sql
CREATE TABLE drugdb.drug (
    formulation_id        UUID  PRIMARY KEY,
    generic_name          TEXT,
    generic_formulation   TEXT,
    dosage_forms          TEXT,
    master_linkage_id     UUID
);
```

> Note: `postgres_schema.sql` shows an older TEXT formulation_id. The actual deployed table uses UUID. All FK columns in child tables use UUID to match.

| Column | Type | Notes |
|--------|------|-------|
| `formulation_id` | UUID PK | Generated per rxnorm entry; one drug can have multiple formulations (e.g. 250 mg and 500 mg) |
| `generic_name` | TEXT | From rxnorm name field |
| `generic_formulation` | TEXT | Strength+form string, e.g. "nelfinavir 250 MG Oral Tablet" |
| `dosage_forms` | TEXT | Dosage form from rxnorm |
| `master_linkage_id` | UUID | FK to `DrugMasterLinkage` — links back to source JSONB |

**Indexes:**
```sql
CREATE INDEX idx_drug_generic ON drug(generic_name);
CREATE INDEX idx_drug_normalized ON drug(normalized_name);
CREATE INDEX idx_drug_class ON drug USING GIN(drug_class);
CREATE INDEX idx_drug_trgm ON drug USING GIN(generic_name gin_trgm_ops);
```

---

### 4.2 `drugdb.drug_identifier`

**Purpose:** Universal identifier lookup. Maps any external ID (rxcui, NDC, UNII, DrugBank ID, etc.) to an internal `formulation_id`. Primary lookup path for the entity resolver.

**Row count:** 578,635  
**Schema file:** `schemas/drug_identifier_schema.sql`  
**Population script:** `scripts/populate_drug_identifier.py`  
**Log files:** `logs/drug_identifier_populate_<timestamp>.log`  
**Source:** `DrugMasterLinkage.combined_clean_jsonb` joined with `drugdb.drug` on `master_linkage_id`

```sql
CREATE TABLE IF NOT EXISTS drugdb.drug_identifier (
    id             SERIAL PRIMARY KEY,
    formulation_id UUID NOT NULL REFERENCES drugdb.drug(formulation_id) ON DELETE CASCADE,
    id_type        TEXT NOT NULL,
    id_value       TEXT NOT NULL,
    UNIQUE(formulation_id, id_type, id_value)
);
```

**Indexes:**

| Index | On | Purpose |
|-------|----|---------|
| `idx_di_lookup` | `(id_type, id_value)` | Primary lookup path for entity resolution |
| `idx_di_formulation` | `(formulation_id)` | Reverse lookup — all IDs for a formulation |
| `idx_di_rxcui` | `(id_value) WHERE id_type='rxcui'` | Fast rxcui-only scan |
| `idx_di_ndc` | `(id_value) WHERE id_type IN ('ndc_product','ndc_package')` | Fast NDC scan |

**id_types populated:**

| id_type | Example value | Granularity | JSONB source path | Coverage |
|---------|---------------|-------------|-------------------|----------|
| `rxcui` | 311924 | Formulation | `openfda.openfda_metadata.rxcui[]` | 59.5% |
| `ndc_product` | 63010-010 | Formulation | `openfda.openfda_metadata.product_ndc[]` | 57.9% |
| `ndc_package` | 63010-010-30 | Package | `openfda.openfda_metadata.package_ndc[]` | 42.7% |
| `unii` | 98D603VP8V | Molecule | `openfda.openfda_metadata.unii[]` | 71.1% |
| `upc` | 0363010010309 | Package | `openfda.openfda_metadata.upc[]` | 23.0% |
| `application_number` | NDA020779 | Drug | `dailymed.drug_info.products[].approval_id` | 56.5% |
| `spl_id` | 4af6d4ca-… | Label | `openfda.openfda_metadata.spl_id[]` | 72.8% |
| `spl_set_id` | e72c2bc6-… | Label Set | `openfda.openfda_metadata.spl_set_id[]` | 72.8% |
| `drugbank` | DB00220 | Drug | `drugbank[].drug_info.drugbank_id` | 94.7% |

**Formulation mapping strategy (strength matching):**

For id_types that exist at the product/NDC level (not formulation level), the script must map each product to a specific `formulation_id` using strength matching:

1. Extract `dailymed.drug_info.products[].active_ingredients[0].strength` → parse numeric mg (e.g. "250 mg" → 250.0)
2. Build `strength_to_rxcui` map from `drugdb.drug` for this `master_linkage_id`
3. Match strength → rxcui → `formulation_id`
4. Fallback: scan imprint string token by token for numeric values matching known strengths
5. Fallback: single-formulation drug → assign directly regardless of strength match
6. No match + multiple formulations → **skip insert** (skip-on-miss policy)

**Skip-on-miss policy rationale:** Allergen extracts and similar drugs have 300+ product entries with gram/unit strengths that don't parse to mg. Without skip-on-miss, one such drug generates ~100,000 cross-mapped rows (302 products × 356 formulations × 2 id_types). A wrong NDC→formulation mapping is worse than no mapping.

**Brand rxcui fallback:** Some rxcuis are brand-level (tty=SBD), referencing no ingredient in the rxnorm array. These are resolved by extracting strength from the `generic_formulation` name string (e.g. "nelfinavir 250 MG Oral Tablet [Viracept]" → 250.0 mg) and mapping to the generic formulation with that strength.

---

### 4.3 `public.active_ingredient`

**Purpose:** One row per active ingredient per formulation. Populated by Pass 2 extraction.

**Row count:** 0 (pending)  
**Population script:** `scripts/run_pass2.py`

```sql
CREATE TABLE active_ingredient (
    id               SERIAL PRIMARY KEY,
    formulation_id   TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    substance_name   TEXT NOT NULL,
    normalized_name  TEXT NOT NULL,
    unii             TEXT,
    drugbank_id      TEXT,
    strength_label   TEXT,          -- "250 mg"
    strength_value   NUMERIC,
    strength_unit    TEXT
);
```

---

### 4.4 `public.inactive_ingredient`

**Purpose:** Excipients, colorants, preservatives per formulation.

**Row count:** 0 (pending)

```sql
CREATE TABLE inactive_ingredient (
    id               SERIAL PRIMARY KEY,
    formulation_id   TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    normalized_name  TEXT NOT NULL,
    unii             TEXT,
    drugbank_id      TEXT,
    role             TEXT           -- excipient, colorant, preservative, etc.
);
```

---

### 4.5 `public.drug_indication`

**Purpose:** Approved indications per formulation with ICD10/SNOMED/MeSH codes and population/line-of-therapy qualifiers.

**Row count:** 0 (pending)

```sql
CREATE TABLE drug_indication (
    id                   SERIAL PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    term                 TEXT NOT NULL,
    icd10                TEXT,
    snomed               TEXT,
    mesh                 TEXT,
    population           TEXT DEFAULT 'any',
    line_of_therapy      TEXT DEFAULT 'unspecified',
    combination_required BOOLEAN DEFAULT FALSE,
    combination_agents   TEXT[] DEFAULT '{}',
    source_section       TEXT,
    source_excerpt       TEXT
);
```

---

### 4.6 `public.drug_interaction`

**Purpose:** Pairwise drug interactions with severity, mechanism, effect quantification.

**Row count:** 0 (pending)

```sql
CREATE TABLE drug_interaction (
    id                     SERIAL PRIMARY KEY,
    interaction_id         TEXT UNIQUE NOT NULL,
    subject_formulation_id TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    subject_substance      TEXT,
    subject_substance_role TEXT DEFAULT 'unknown',
    partner_name           TEXT NOT NULL,
    partner_rxcui          TEXT,
    partner_drugbank_id    TEXT,
    partner_drug_class     TEXT,
    severity               TEXT DEFAULT 'unknown',    -- contraindicated, major, moderate, minor
    effect_direction       TEXT,                      -- increase, decrease, no_change, unclear
    effect_on              TEXT,
    magnitude              TEXT,                      -- e.g. "↑AUC 505% (393–643)"
    mechanism              TEXT,                      -- e.g. "CYP3A4 inhibition"
    clinical_management    TEXT,
    evidence_level         TEXT DEFAULT 'established',
    source                 TEXT NOT NULL,
    source_document_id     TEXT,
    source_section         TEXT,
    source_excerpt         TEXT
);
```

---

### 4.7 `public.contraindication`

**Purpose:** Absolute and relative contraindications per formulation (condition-based, drug-based, population-based, allergy-based).

**Row count:** 0 (pending)

```sql
CREATE TABLE contraindication (
    id               SERIAL PRIMARY KEY,
    formulation_id   TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    kind             TEXT NOT NULL,    -- condition, coadministered_drug, population, allergy
    term             TEXT NOT NULL,
    rxcui            TEXT,
    drugbank_id      TEXT,
    drug_class       TEXT,
    reason           TEXT,
    severity         TEXT DEFAULT 'absolute',
    source_section   TEXT,
    source_excerpt   TEXT
);
```

---

### 4.8 `public.dosing_regimen`

**Purpose:** Structured dosing rules per formulation with full population filters (age, weight, sex, renal/hepatic function, pregnancy).

**Row count:** 0 (pending)

```sql
CREATE TABLE dosing_regimen (
    id                   SERIAL PRIMARY KEY,
    regimen_id           TEXT UNIQUE NOT NULL,
    formulation_id       TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    indication           TEXT,
    age_group            TEXT DEFAULT 'any',
    age_min_years        NUMERIC,
    age_max_years        NUMERIC,
    weight_min_kg        NUMERIC,
    weight_max_kg        NUMERIC,
    sex                  TEXT DEFAULT 'any',
    pregnancy_status     TEXT DEFAULT 'any',
    renal_function       TEXT DEFAULT 'any',
    hepatic_function     TEXT DEFAULT 'any',
    route                TEXT,
    dose_amount          TEXT,          -- "1250 mg", "45-55 mg/kg", "CONTRAINDICATED"
    dose_value           NUMERIC,
    dose_unit            TEXT,
    dose_basis           TEXT,          -- fixed, per_kg, per_m2, titrated
    frequency            TEXT,          -- BID, TID, QD, q8h
    duration             TEXT,
    max_daily_dose       TEXT,
    administration_notes TEXT,
    adjustment_required_for TEXT[] DEFAULT '{}',
    source_section       TEXT,
    source_excerpt       TEXT
);
```

---

### 4.9 `public.population_approval`

**Purpose:** Regulatory approval status by population (pediatric, geriatric, pregnant, lactating).

**Row count:** 0 (pending)

```sql
CREATE TABLE population_approval (
    id                 SERIAL PRIMARY KEY,
    formulation_id     TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    population         TEXT NOT NULL,
    status             TEXT NOT NULL,    -- approved, studied_not_approved, not_studied, contraindicated
    approved_age_range TEXT,
    pregnancy_category TEXT,             -- A, B, C, D, X
    has_registry       BOOLEAN DEFAULT FALSE,
    notes              TEXT,
    source_section     TEXT,
    source_excerpt     TEXT,
    UNIQUE(formulation_id, population)
);
```

---

### 4.10 `public.administration_timing`

**Purpose:** Food requirements and drug separation timing per formulation.

**Row count:** 0 (pending)

```sql
CREATE TABLE administration_timing (
    id                  SERIAL PRIMARY KEY,
    formulation_id      TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    food_requirement    TEXT,    -- with_food, empty_stomach, either, unknown
    food_details        TEXT,
    drug_separations    JSONB DEFAULT '[]',
    other_timing_notes  TEXT,
    source              TEXT DEFAULT 'regex',
    UNIQUE(formulation_id)
);
```

---

### 4.11 `public.available_strength`

**Purpose:** All commercially available strengths per formulation with rxcui linkage.

**Row count:** 0 (pending)

```sql
CREATE TABLE available_strength (
    id               SERIAL PRIMARY KEY,
    formulation_id   TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    strength_value   NUMERIC NOT NULL,
    strength_unit    TEXT NOT NULL,
    strength_label   TEXT NOT NULL,    -- "250 MG"
    rxcui            TEXT,
    dosage_form      TEXT
);
```

---

### 4.12 `public.adverse_event`

**Purpose:** Adverse event profiles by MedDRA preferred term with frequency/incidence data.

**Row count:** 0 (pending)

```sql
CREATE TABLE adverse_event (
    id               SERIAL PRIMARY KEY,
    formulation_id   TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    term             TEXT NOT NULL,
    meddra_pt        TEXT,
    system_organ_class TEXT,
    frequency        TEXT,           -- very_common, common, uncommon, rare, very_rare, unknown
    incidence_pct    NUMERIC,
    population       TEXT,
    seriousness      TEXT,
    source_section   TEXT,
    source_excerpt   TEXT
);
```

---

### 4.13 `public.warning`

**Purpose:** Boxed warnings, warnings, and precautions per formulation.

**Row count:** 0 (pending)

```sql
CREATE TABLE warning (
    id               SERIAL PRIMARY KEY,
    formulation_id   TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    warning_type     TEXT NOT NULL,    -- boxed_warning, warning, precaution
    topic            TEXT,
    text             TEXT,
    source_section   TEXT
);
```

---

### 4.14 `drugdb.clinical_section`

**Purpose:** Full-text narrative label sections (indications, dosing, interactions, warnings, etc.) from both OpenFDA and DailyMed sources. Primary input for RAG chunking and embedding.

**Row count:** 2,887,910 ✅ (populated 2026-05-06)
**Population script:** `populate_clinical_section.py`
**Report:** `reports/clinical_section_population_report.md`

```sql
CREATE TABLE IF NOT EXISTS drugdb.clinical_section (
    id                 SERIAL PRIMARY KEY,
    formulation_id     UUID NOT NULL
                       REFERENCES drugdb.drug(formulation_id)
                       ON DELETE CASCADE,
    section            TEXT NOT NULL,
    text               TEXT,
    subsections        JSONB DEFAULT '[]',
    source             TEXT,                -- 'openfda' or 'dailymed'
    source_document_id TEXT,
    UNIQUE(formulation_id, section, source)
);

CREATE INDEX IF NOT EXISTS idx_cs_formulation ON drugdb.clinical_section(formulation_id);
CREATE INDEX IF NOT EXISTS idx_cs_section     ON drugdb.clinical_section(section);
```

| Column | Type | Notes |
|--------|------|-------|
| `formulation_id` | UUID FK | References `drugdb.drug(formulation_id)` |
| `section` | TEXT | Section key name e.g. `indications_and_usage`, `boxed_warning` |
| `text` | TEXT | Full narrative text; NULL when empty |
| `subsections` | JSONB | Array of `{subsection_id, title, text}` objects; `[]` for OpenFDA rows |
| `source` | TEXT | `openfda` or `dailymed` — both stored as separate rows |
| `source_document_id` | TEXT | OpenFDA: `set_id`; DailyMed: `drug_label.document_id` |

**Breakdown:**
- OpenFDA rows: 1,840,508 (text from `child["text"]`, subsections always `[]`)
- DailyMed rows: 1,047,402 (text from `child["content"]`, subsections transformed)
- 45 unique sections discovered dynamically across 9 parent keys
- 2,492 DrugMasterLinkage records skipped (no matching `drugdb.drug` row — known data gap)

---

### 4.15 `public.label_table`

**Purpose:** Structured tables extracted from drug labels (dosing tables, interaction tables, PK tables).

**Row count:** 0 (pending)

```sql
CREATE TABLE label_table (
    id               SERIAL PRIMARY KEY,
    formulation_id   TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    table_id         TEXT NOT NULL,
    caption          TEXT,
    semantic_type    TEXT,    -- dosing, interaction, adverse_event, pharmacokinetics, clinical_study
    section          TEXT,
    headers          TEXT[],
    rows_data        JSONB DEFAULT '[]'
);
```

---

### 4.16 `public.rxnorm_formulation`

**Purpose:** RxNorm-normalized formulation records with TTY classification (SCD, SBD, SCDC, SBDC, GPCK, BPCK).

**Row count:** 0 (pending)

```sql
CREATE TABLE rxnorm_formulation (
    id               SERIAL PRIMARY KEY,
    formulation_id   TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    rxcui            TEXT NOT NULL,
    tty              TEXT,           -- SCD, SBD, SCDC, SBDC, GPCK, BPCK
    name             TEXT,
    kind             TEXT,           -- generic, brand
    dose_form        TEXT,
    strength_value   NUMERIC,
    strength_unit    TEXT,
    strength_label   TEXT,
    synonyms         TEXT[] DEFAULT '{}'
);
```

---

### 4.17 `public.rag_chunk`

**Purpose:** Chunked, embedded clinical text for hybrid vector + metadata search. One row per chunk with 1024-dimensional bge-large-en-v1.5 embedding.

**Row count:** 0 (pending — after embedding run)

```sql
CREATE TABLE rag_chunk (
    chunk_id             TEXT PRIMARY KEY,
    formulation_id       TEXT NOT NULL REFERENCES drug(formulation_id) ON DELETE CASCADE,
    section              TEXT,
    subsection_id        TEXT,
    subsection_title     TEXT,
    semantic_type        TEXT NOT NULL,
    source               TEXT,
    text                 TEXT NOT NULL,
    generic_name         TEXT,
    brand_names          TEXT[],
    rxcui                TEXT[],
    drugbank_ids         TEXT[],
    manufacturer         TEXT,
    routes               TEXT[],
    dosage_forms         TEXT[],
    partner_name         TEXT,
    partner_drugbank_id  TEXT,
    subject_substance    TEXT,
    subject_substance_role TEXT,
    severity             TEXT,
    embedding            vector(1024)
);
-- IVFFlat index created AFTER all embeddings are loaded:
-- CREATE INDEX idx_rc_embedding ON rag_chunk
--   USING ivfflat (embedding vector_cosine_ops) WITH (lists = 1000);
```

---

### 4.18 `public.indian_brand`

**Purpose:** Indian brand names with manufacturer, strength, schedule, MRP. Linked to `drug.formulation_id` by fuzzy name matching.

**Row count:** 0 (pending)  
**Population scripts:** `scripts/indian_brand_loader.py` → `scripts/indian_brand_mapper.py`

```sql
CREATE TABLE indian_brand (
    indian_brand_id      SERIAL PRIMARY KEY,
    brand_name           TEXT NOT NULL,
    manufacturer_india   TEXT,
    generic_name_raw     TEXT NOT NULL,
    normalized_generic_name TEXT NOT NULL,
    strength_label       TEXT,
    strength_value       NUMERIC,
    strength_unit        TEXT,
    dosage_form_raw      TEXT,
    form_canonical       TEXT,
    route                TEXT DEFAULT 'ORAL',
    pack_size            TEXT,
    schedule             TEXT,       -- H, H1, X, G, OTC
    mrp_inr              NUMERIC,
    cdsco_approval       BOOLEAN DEFAULT TRUE,
    is_combination       BOOLEAN DEFAULT FALSE,
    formulation_id       TEXT REFERENCES drug(formulation_id),
    match_confidence     TEXT        -- exact, normalized, fuzzy, manual
);
```

---

### 4.19 `public.indian_brand_ingredient`

**Purpose:** Per-ingredient rows for fixed-dose combination (FDC) Indian brands. Each ingredient links to its own `formulation_id`.

**Row count:** 0 (pending)

```sql
CREATE TABLE indian_brand_ingredient (
    id                   SERIAL PRIMARY KEY,
    indian_brand_id      INT NOT NULL REFERENCES indian_brand(indian_brand_id) ON DELETE CASCADE,
    ingredient_index     INT NOT NULL,
    ingredient_name      TEXT NOT NULL,
    ingredient_strength  TEXT,
    formulation_id       TEXT REFERENCES drug(formulation_id),
    match_confidence     TEXT
);
```

---

### 4.20 `public.query_audit_log`

**Purpose:** Logs every API query with inputs, resolved drugs, SQL/graph/RAG results, LLM prompts and responses, for audit and debugging.

```sql
CREATE TABLE query_audit_log (
    id                   BIGSERIAL PRIMARY KEY,
    query_template       TEXT NOT NULL,    -- Q1, Q2, ... Q9
    request_payload      JSONB NOT NULL,
    resolved_drugs       JSONB,
    sql_results          JSONB,
    graph_results        JSONB,
    rag_chunks_used      TEXT[],
    llm_prompt           TEXT,
    llm_response         TEXT,
    indian_brands_shown  JSONB,
    response_payload     JSONB,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    response_time_ms     INT
);
```

---

## 5. Population Scripts Reference

| Script | Tables written | Run command | Dependencies |
|--------|---------------|-------------|--------------|
| `normalize_openfda.py` | — (intermediate) | `python3 scripts/normalize_openfda.py` | openFDA JSON files |
| `normalize_dailymed.py` | — (intermediate) | `python3 scripts/normalize_dailymed.py` | DailyMed XML |
| `normalize_drugbank.py` | — (intermediate) | `python3 scripts/normalize_drugbank.py` | DrugBank XML |
| `standardize_records.py` | — (intermediate) | `python3 scripts/standardize_records.py` | Above 3 |
| `transform_to_unified.py` | `DrugMasterLinkage` | `python3 scripts/transform_to_unified.py` | All standardized |
| `populate_drug_table.py` | `drugdb.drug` | `python3 scripts/populate_drug_table.py --password <PWD>` | `DrugMasterLinkage` |
| `populate_drug_identifier.py` | `drugdb.drug_identifier` | `python3 scripts/populate_drug_identifier.py --password <PWD> --log-dir logs` | `drugdb.drug` + schema |
| `run_pass2.py` | 12 fact tables | `python3 scripts/run_pass2.py --password <PWD>` | `drugdb.drug`, vLLM endpoint |
| `chunk_for_rag.py` | `public.clinical_section` → chunks | `python3 scripts/chunk_for_rag.py` | `clinical_section` |
| `embed_chunks.py` | `public.rag_chunk` | `python3 scripts/embed_chunks.py` | TEI embedding server, chunks |
| `indian_brand_loader.py` | `public.indian_brand` | `python3 scripts/indian_brand_loader.py --password <PWD>` | CSV file |
| `indian_brand_mapper.py` | `indian_brand.formulation_id` | `python3 scripts/indian_brand_mapper.py --password <PWD>` | `drugdb.drug`, `indian_brand` |
| `neo4j_populate.py` | Neo4j nodes + rels | `python3 scripts/neo4j_populate.py` | `drug_interaction`, `drug` |

**Schema must be applied before running population scripts:**
```bash
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/drug_identifier_schema.sql
```

**Run order for completed stages:**
```bash
python3 scripts/populate_drug_table.py --password Admin@123 --log-file logs/populate_drug_table.log
python3 scripts/populate_drug_identifier.py --password Admin@123 --log-dir logs
```

---

## 6. Run History — drug_identifier

This section documents the full population run of `drugdb.drug_identifier` on 2026-05-05, including all errors encountered during development and how each was resolved.

### 6.1 Architecture decisions

**Two-connection pattern** (key design decision):

The script uses two separate psycopg2 connections to avoid a named cursor invalidation bug:

```python
read_conn = psycopg2.connect(**db_kwargs)
read_conn.autocommit = False    # Never committed — holds server-side cursor's transaction
psycopg2.extras.register_default_jsonb(read_conn)

write_conn = psycopg2.connect(**db_kwargs)
write_conn.autocommit = False
write_cur = write_conn.cursor()
```

PostgreSQL server-side (named) cursors live within the transaction of their connection. Any `COMMIT` on that connection destroys the cursor mid-stream. The solution: read and write never share a connection; `read_conn` is never committed.

**Server-side streaming cursor:**
```python
stream_cur = read_conn.cursor(name="drug_identifier_stream")
stream_cur.itersize = 500
stream_cur.execute(stream_sql)
```

This allows the JOIN across `DrugMasterLinkage` (~50K rows, each with a large JSONB blob) and `drugdb.drug` (88,983 rows) without loading any blobs into Python memory.

**Batch flush with ON CONFLICT DO NOTHING:**
```python
cursor.executemany("""
    INSERT INTO drugdb.drug_identifier (formulation_id, id_type, id_value)
    VALUES (%s, %s, %s)
    ON CONFLICT (formulation_id, id_type, id_value) DO NOTHING
""", unique_batch)
```

Batch size = 5,000. In-memory `set()` deduplication before each batch prevents duplicate inserts in the same batch.

---

### 6.2 Errors encountered and resolutions

#### Error 1: Named cursor invalidated by commit

**Symptom:**
```
psycopg2.ProgrammingError: named cursor isn't valid anymore
```

**Cause:** The initial implementation used a single connection for both the streaming cursor and batch writes. `write_conn.commit()` after each batch invalidated the named cursor on the same connection.

**Resolution:** Split into two connections (read_conn / write_conn as described above). `read_conn` holds a permanent open transaction for the duration of the entire stream; `write_conn` commits each batch independently.

---

#### Error 2: SQL syntax error in no-limit query path

**Symptom:**
```
psycopg2.errors.SyntaxError: syntax error at or near "JOIN"
```

**Cause:** The else-branch (no `--limit` flag) embedded `WHERE dml.combined_clean_jsonb IS NOT NULL` inside a subquery but before the JOIN keyword in the outer query:

```sql
-- BROKEN:
SELECT ... FROM public."DrugMasterLinkage" dml
WHERE dml.combined_clean_jsonb IS NOT NULL
JOIN drugdb.drug d ON ...   -- ← syntax error: WHERE before JOIN
```

**Resolution:** Moved the WHERE clause to after the JOIN in the outer query. Used a subquery only for the `--limit` path:

```sql
-- LIMIT path:
SELECT ... FROM (
    SELECT ... FROM "DrugMasterLinkage"
    WHERE combined_clean_jsonb IS NOT NULL
    ORDER BY master_linkage_id LIMIT {n}
) dml JOIN drugdb.drug d ON ...

-- Full path:
SELECT ... FROM "DrugMasterLinkage" dml
JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
WHERE dml.combined_clean_jsonb IS NOT NULL
ORDER BY dml.master_linkage_id
```

---

#### Error 3: Application_number cross-mapping (duplicate/wrong inserts)

**Symptom:** Each formulation was getting all application_numbers from the JSONB, not just the one from its matched product.

**Cause:** A redundant loop at the bottom of the per-master_id block iterated over `openfda_metadata.application_number[]` and inserted every application_number for every formulation:

```python
# WRONG — inserted ALL app_nums for ALL formulations:
for app_num in (openfda_meta.get("application_number") or []):
    for fid in all_fids:
        rows.append((fid, "application_number", app_num))
```

This ran on top of the correct per-product DailyMed loop that already extracted application_number with proper strength-matched formulation_id.

**Resolution:** Removed the redundant loop entirely. Application_numbers are now only inserted via the DailyMed products loop, which uses strength matching to assign the correct `formulation_id`.

---

#### Error 4: Nelfinavir "V;625" imprint not parsed

**Symptom:** The Nelfinavir drug (brand name Viracept) has two formulations: 250 mg and 625 mg. The DailyMed record had `active_ingredients: []` (empty) so the only strength signal was the imprint field: `"V;625"`.

`parse_strength_mg("V;625")` returned `None` because the string contains no "mg" unit. As a result, the 625 mg formulation got no ndc_product or application_number rows, while 250 mg correctly matched via the rxnorm brand formulation name.

**Resolution:** Added context-aware imprint token scanning. After `parse_strength_mg` fails, the script tokenizes the imprint by `[;,\s]+` and checks each token:

```python
if fid is None and imprint:
    for token in re.split(r'[;,\s]+', str(imprint)):
        try:
            val = float(token)
            if val in strength_to_rxcui:
                fid = fid_by_rxcui.get(strength_to_rxcui[val])
                if fid:
                    break
        except ValueError:
            pass
```

`"V;625"` → tokens `["V", "625"]` → `float("625")` = 625.0 → found in `strength_to_rxcui` → correct formulation_id assigned.

**Verification:** Spot-check query confirms nelfinavir has both 250mg and 625mg rows for all id_types.

---

#### Error 5: Combinatorial explosion on allergen extract drug

**Symptom:** Log showed row count jumping from 19,226 to 128,180 (a gain of ~108,000 rows) between drug record #1197 and drug record #1199. With correct skip-on-miss behavior, a single drug should never add more than ~1,000 rows.

**Cause:** Drug `06b393d6` (an allergen extract concentrate) has:
- 356 formulations in `drugdb.drug`
- 302 DailyMed products, each with `active_ingredients[0].strength = "1 g"` (grams, not mg)
- `parse_strength_mg("1 g")` returns `None` for all 302 products
- The old fallback logic: when no strength match, fan out to **all** formulations:

```python
# WRONG old fan-out:
for fid in all_fids:
    rows.append((fid, "ndc_product", ndc_code))
    rows.append((fid, "application_number", app_num))
```

302 products × 356 formulations × 2 id_types = **215,024 wrong rows** for one drug. The UNIQUE constraint reduced the final insertion to ~108,000 unique rows, but all were incorrect cross-mappings.

**Resolution:** Replaced fan-out with skip-on-miss. When strength matching fails and there is more than one formulation, the script skips ndc_product, application_number, and ndc_package inserts entirely:

```python
if fid is not None:
    rows.append((fid, "ndc_product", ndc_code))
    # ... application_number, ndc_package
elif len(all_fids) == 1:
    rows.append((all_fids[0], "ndc_product", ndc_code))
    # single formulation — safe to assign directly
else:
    # Multiple formulations, no strength match — skip to avoid cross-mapping
    logger.debug(f"Skipping ndc_product/app_num for ndc={ndc_code}: "
                 f"no strength match among {len(all_fids)} formulations")
```

This means drugs with non-mg strengths (gram, mL, unit-based allergen extracts) will have lower NDC/application_number coverage, but all mappings that do exist are correct.

---

### 6.3 Final run results (2026-05-05)

| Metric | Value |
|--------|-------|
| DrugMasterLinkage groups processed | 47,619 |
| Drug formulations covered (drugdb.drug) | 88,983 |
| DML records with no drug rows (correctly skipped) | 2,492 |
| Rows extracted | 579,896 |
| Rows inserted (after dedup + ON CONFLICT) | 578,635 |
| Insert errors | 0 |
| Log file | `logs/drug_identifier_populate_20260505_084552.log` |

**Note on "47,619 vs 88,983" count:** The log reports "drugs processed: 47,619". This is the number of distinct `master_linkage_id` groups iterated by `itertools.groupby` on the streaming cursor. Each group covers all its formulations. All 88,983 rows in `drugdb.drug` were covered — the 47,619 figure counts distinct DrugMasterLinkage source records, not individual formulation rows.

**Coverage by id_type (from verification query):**

| id_type | Rows | % of drug table covered |
|---------|------|------------------------|
| `spl_set_id` | ~65K | 72.8% |
| `spl_id` | ~65K | 72.8% |
| `drugbank` | ~84K | 94.7% |
| `unii` | ~63K | 71.1% |
| `rxcui` | ~53K | 59.5% |
| `ndc_product` | ~51K | 57.9% |
| `application_number` | ~50K | 56.5% |
| `ndc_package` | ~38K | 42.7% |
| `upc` | ~21K | 23.0% |

---

## 7. Verification Queries

```sql
-- Total rows in drug_identifier
SELECT COUNT(*) FROM drugdb.drug_identifier;
-- Expected: 578,635

-- Breakdown by id_type
SELECT id_type, COUNT(*) AS count
FROM drugdb.drug_identifier
GROUP BY id_type ORDER BY count DESC;

-- Coverage % per id_type (formulations with at least one entry)
SELECT
    id_type,
    COUNT(DISTINCT formulation_id) AS formulations_covered,
    ROUND(COUNT(DISTINCT formulation_id) * 100.0
          / (SELECT COUNT(*) FROM drugdb.drug), 2) AS pct_coverage
FROM drugdb.drug_identifier
GROUP BY id_type ORDER BY pct_coverage DESC;

-- Zero orphans (must return 0 — confirms all FK references are valid)
SELECT COUNT(*) FROM drugdb.drug_identifier di
LEFT JOIN drugdb.drug d ON di.formulation_id = d.formulation_id
WHERE d.formulation_id IS NULL;

-- Spot-check Nelfinavir / Viracept (confirms both 250mg and 625mg entries)
SELECT d.generic_formulation, di.id_type, di.id_value
FROM drugdb.drug_identifier di
JOIN drugdb.drug d ON di.formulation_id = d.formulation_id
WHERE d.generic_name ILIKE '%nelfinavir%'
ORDER BY d.generic_formulation, di.id_type;

-- Count drug rows (should be ~88,983)
SELECT COUNT(*) FROM drugdb.drug;

-- Count distinct master_linkage_ids in drug (should be ~47,619)
SELECT COUNT(DISTINCT master_linkage_id) FROM drugdb.drug;

-- DML records with no drug rows (correctly skipped during population)
SELECT COUNT(*) FROM public."DrugMasterLinkage" dml
LEFT JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
WHERE d.formulation_id IS NULL
  AND dml.combined_clean_jsonb IS NOT NULL;
-- Expected: ~2,492
```
