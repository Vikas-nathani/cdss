# CDSS Database Deep Research Report

**Generated:** 2026-05-04 07:32:29 UTC  
**Database:** postgres @ 178.236.185.230  
**Schemas Analyzed:** `drugdb`, `public`  
**Tables Analyzed:** 8

---

## Executive Summary

| Table | Rows | Key Coverage | Notes |
|-------|------|-------------|-------|
| `drugdb.ingredients` | 20,037 | drugbank_id: 99.1% / rxcui: 10.7% | 190 skeleton rows |
| `drugdb.ingredient_synonyms` | 18,160 | 100% non-null | Avg 2.87 synonyms/ingredient |
| `drugdb.indian_brand_ingredient` | 580,669 | drugbank_id: 99.17% | 4,830 rows unmapped |
| `public.DrugSourceMaster` | 738,197 | — | Source registry |
| `public.DrugMasterLinkage` | 50,111 | JSONB: 100.0% | Central linking table |
| `drugdb.drug` | 88,983 | rxcui: 100.0% | 0 rows missing generic_name |
| `drugdb.drug_ingredient_mapping` | 98,832 | formulation coverage: 99.84% | 144 formulations uncovered |
| `drugdb.drug_synonym_formulation` | 66,154 | formulation coverage: 74.34% | Max 48 synonyms per row |

**Key Findings:**
- `drugdb.drug` is the hub: 88,983 formulations from 47,619 / 50,111 DrugMasterLinkage records (95.0%)
- Ingredient mapping covers **99.84%** of formulations; synonym coverage **74.34%**
- 66,018 formulations have both ingredient mapping AND synonyms; 8 have neither
- `drugdb.ingredients` has **190 skeleton rows** (rxcui filled, drugbank_id NULL) and **17,900** DrugBank-only rows
- `drugdb.indian_brand_ingredient` coverage: **99.17%** — 4,830 rows still unmapped

---

## Table of Contents

1. [drugdb.ingredients](#1-drugdbingredients)
2. [drugdb.ingredient_synonyms](#2-drugdbingredient_synonyms)
3. [drugdb.indian_brand_ingredient](#3-drugdbindian_brand_ingredient)
4. [public.DrugSourceMaster](#4-publicdrugsourcemaster)
5. [public.DrugMasterLinkage](#5-publicdrugmasterlinkage)
6. [drugdb.drug](#6-publicdrug)
7. [drugdb.drug_ingredient_mapping](#7-publicdrug_ingredient_mapping)
8. [drugdb.drug_synonym_formulation](#8-publicdrug_synonym_formulation)
9. [Entity Relationship Diagram](#9-entity-relationship-diagram)
10. [Cross-Table Coverage Summary](#10-cross-table-coverage-summary)
11. [Known Data Quality Issues](#11-known-data-quality-issues)
12. [Scripts Reference Summary](#12-scripts-reference-summary)

---

## 1. drugdb.ingredients

### Purpose
Pre-existing DrugBank ingredient reference table enriched by the CDSS pipeline with RxNorm RxCUI codes. Serves as the canonical ingredient lookup for `drugdb.drug_ingredient_mapping`. Contains ~20,034 rows: the majority are full DrugBank entries; ~190 are skeleton rows (RxNorm-only, no DrugBank equivalent).

### Schema

| Column | Type | Nullable | Default | PK | FK References |
|--------|------|----------|---------|----|----|
| `id` | uuid | NO | gen_random_uuid() | ✓ |  |
| `drugbank_id` | character varying(50) | YES |  |  |  |
| `unii` | character varying(50) | YES |  |  |  |
| `rxcui` | character varying(50) | YES |  |  |  |
| `name` | character varying(255) | NO |  |  |  |
| `indications` | text | YES |  |  |  |
| `general_function` | text | YES |  |  |  |
| `type` | USER-DEFINED | YES |  |  |  |
| `pharmacodynamics` | text | YES |  |  |  |
| `classification_description` | text | YES |  |  |  |
| `food_interactions` | text | YES |  |  |  |
| `created_at` | timestamp with time zone | YES | now() |  |  |
| `updated_at` | timestamp with time zone | YES | now() |  |  |
| `created_by` | character varying(255) | YES |  |  |  |

### Indexes & Constraints

- `ingredients_pkey`: `CREATE UNIQUE INDEX ingredients_pkey ON drugdb.ingredients USING btree (id)`
- `idx_ingredients_drugbank`: `CREATE INDEX idx_ingredients_drugbank ON drugdb.ingredients USING btree (drugbank_id)`
- `idx_ingredients_unii`: `CREATE INDEX idx_ingredients_unii ON drugdb.ingredients USING btree (unii)`
- `idx_ingredients_rxcui`: `CREATE INDEX idx_ingredients_rxcui ON drugdb.ingredients USING btree (rxcui)`
- `idx_ingredients_name`: `CREATE INDEX idx_ingredients_name ON drugdb.ingredients USING btree (name)`
- `idx_ingredients_updated_at`: `CREATE INDEX idx_ingredients_updated_at ON drugdb.ingredients USING btree (updated_at)`

### Data Source & Derivation

| Layer | Source |
|-------|--------|
| Base data | Pre-existing DrugBank export via `drugdb_migration.sql` |
| `rxcui` UPDATE | `DrugMasterLinkage.combined_clean_jsonb → rxnorm[] → ingredients[] → ing_rxcui` |
| `rxcui` INSERT (skeleton rows) | Same JSONB path; inserted when no DrugBank match found |
| `drugbank_id` fill (5 rows, 2026-05-04) | `fuzzy_ingredient_dedup.py` Phase 2 synonym/fuzzy match |

### Population Script
**`update_ingredient_rxcui.py`** — Extracts `(ingredient_name, ing_rxcui)` pairs from `DrugMasterLinkage`. Matches via 4-tier strategy (exact name → prefix name → exact synonym → prefix synonym). Updates `rxcui`; inserts skeleton rows for unmatched RxNorm-only ingredients. Idempotent.  
**`fuzzy_ingredient_dedup.py`** — Fuzzy dedup: 195 skeleton rows vs DrugBank rows. Phase 2 filled `drugbank_id` on 5 skeleton rows (2026-05-04).

### Column-Level Mapping

| Column | Source Table | JSONB Path / Transformation | Example Values | NULL Count | NULL % |
|--------|-------------|----------------------------|----------------|-----------|--------|
| `id` | DrugBank migration | UUID assigned at import | `96201432-d148...` | 0 | 0% |
| `name` | DrugBank migration | Raw drug name | `Aspirin`, `Metformin`, `Ethinylestradiol` | 0 | 0% |
| `drugbank_id` | DrugBank migration | e.g. DB00945 | `DB14476`, `DB00945`, `DB00316` | 190 | 0.9% |
| `rxcui` | RxNorm via JSONB | `rxnorm[].ingredients[].ing_rxcui` | `237099`, `1191`, `860975` | 17,900 | 89.3% |
| `unii` | DrugBank migration | FDA UNII code | `H0G9379FGK`, `R16CO5Y76E` | 195 | 1.0% |
| `indications` | DrugBank migration | Free text | Clinical indication text | 195 | 1.0% |
| `general_function` | DrugBank migration | Free text | `Serine protease inhibitor` | 195 | 1.0% |
| `pharmacodynamics` | DrugBank migration | Free text | Pharmacological description | 195 | 1.0% |
| `classification_description` | DrugBank migration | Free text | `Organic compounds` | 195 | 1.0% |
| `food_interactions` | DrugBank migration | JSONB array | `["Avoid alcohol"]` | 18,584 | 92.7% |
| `created_by` | Script metadata | Script name string | `update_ingredient_rxcui.py` | 195 | 1.0% |

### Statistics

| Metric | Value |
|--------|-------|
| Total rows | 20,037 |
| With `drugbank_id` | 19,847 (99.1%) |
| With `rxcui` | 2,137 (10.7%) |
| With `unii` | 19,842 (99.0%) |
| With `indications` | 19,842 (99.0%) |
| With `general_function` | 19,842 (99.0%) |
| With `pharmacodynamics` | 19,842 (99.0%) |
| With `classification_description` | 19,842 (99.0%) |
| With `food_interactions` | 1,453 (7.3%) |
| **Skeleton rows** (rxcui ✓, drugbank_id ✗) | **190** |
| Fully mapped (both rxcui + drugbank_id) | 1,947 |
| DrugBank-only (rxcui NULL) | 17,900 |
| Neither (rxcui NULL + drugbank_id NULL) | 0 |
| Name min / max / avg length | 3 / 226 / 22.73 chars |

**Rows by `created_by`:**

| created_by | count |
|------------|-------|
| admin | 19,842 |
| NULL | 195 |

### Sample Rows

| id | name | drugbank_id | rxcui | unii | created_by |
|---||---||---||---||---||---|
| c79436d0-5acb-4b94-b55b-107da2f208dd | 17-alpha-hydroxyprogesterone | NULL | 5542 | NULL | NULL |
| fd548b68-dc2b-5fc1-97f5-29657f9aa552 | Aminocaproic acid | DB00513 | 99 | U6F3787206 | admin |
| bc4c1289-12d7-56f5-a45c-3457c18c9ed5 | Abacavir | DB01048 | 190521 | WR2TIP26VS | admin |
| 2c4bcbd3-63d6-52dd-bd79-3f302ccb39eb | Abaloparatide | DB05084 | 1921069 | AVK0I6HY2U | admin |
| b7963f00-a1f6-57a6-85a4-2523693e42c4 | Abametapir | DB11932 | 2475532 | 6UO390AMFB | admin |

### Sample Skeleton Rows (rxcui set, drugbank_id NULL)

| id | name | rxcui | drugbank_id |
|---||---||---||---|
| c79436d0-5acb-4b94-b55b-107da2f208dd | 17-alpha-hydroxyprogesterone | 5542 | NULL |
| f64bcfe9-6b8a-4ef5-bc18-aa998dde71c7 | Acacia pollen extract | 851732 | NULL |
| 1588fbf4-5c3a-4400-a994-fc9b1ef1dded | albumin human, USP | 828529 | NULL |
| b3e9b019-ef6d-4365-823f-2874b8ee2025 | almond allergenic extract | 892507 | NULL |
| 812777d2-c410-4f7b-b279-59c441b427f2 | aloe polysaccharide | 476786 | NULL |

### Expected vs Actual

| Expectation | Actual | Gap |
|------------|--------|-----|
| All rows have `name` | 20,037 / 20,037 | 0 missing |
| All DrugBank rows have `drugbank_id` | 19,847 have it | 190 skeleton rows lack it |
| ~10.7% rows with rxcui (expected) | 10.7% | As expected |
| ~17,900 DrugBank-only (rxcui NULL) | 17,900 | On target |

### Data Quality Issues

- **190 skeleton rows**: rxcui set, drugbank_id NULL. 5 resolved 2026-05-04 by `fuzzy_ingredient_dedup.py`. 185 remain.
- **0 rows with neither**: both rxcui and drugbank_id NULL — likely experimental drug entries.
- 195 rows lack `indications` — expected for skeleton/experimental rows.
- The 17,900 DrugBank-only rows with NULL rxcui are **expected** (no RxNorm equivalent).

### Relationships to Other Tables

| Related Table | Join Column | Type | Coverage |
|---------------|-------------|------|----------|
| `drugdb.ingredient_synonyms` | `id = id` | One-to-many | 18,160 synonym IDs reference ingredients |
| `drugdb.drug_ingredient_mapping` | `id = ingredient_id` | One-to-many | 2,107 distinct ingredient IDs used |
| `drugdb.indian_brand_ingredient` | `drugbank_id = drugbank_id` | One-to-many | 1,627 drugbank_ids matched |

---

## 2. drugdb.ingredient_synonyms

### Purpose
Stores all known synonyms for DrugBank ingredients. Used as a fallback lookup when RxNorm ingredient names don't exactly match DrugBank canonical names. Critical for `second_pass_ingredient_mapping.py`.

### Schema

| Column | Type | Nullable | Default | PK | FK References |
|--------|------|----------|---------|----|----|
| `id` | uuid | NO |  | ✓ | drugdb.ingredients.id |
| `synonym` | character varying(500) | NO |  | ✓ |  |
| `created_at` | timestamp with time zone | YES | now() |  |  |
| `updated_at` | timestamp with time zone | YES | now() |  |  |
| `created_by` | character varying(255) | YES |  |  |  |

### Indexes & Constraints

- `ingredient_synonyms_pkey`: `CREATE UNIQUE INDEX ingredient_synonyms_pkey ON drugdb.ingredient_synonyms USING btree (id, synonym)`
- `idx_synonyms_id`: `CREATE INDEX idx_synonyms_id ON drugdb.ingredient_synonyms USING btree (id)`

### Data Source & Derivation

| Layer | Source |
|-------|--------|
| Base data | Pre-existing DrugBank synonym export via `drugdb_migration.sql` |
| Join key | `id` → `drugdb.ingredients.id` (direct UUID match, not a separate FK column) |

### Column-Level Mapping

| Column | Source | Transformation | Example Values | NULL Count | NULL % |
|--------|--------|----------------|----------------|-----------|--------|
| `id` | DrugBank migration | UUID same as `drugdb.ingredients.id` | `96201432-d148...` | 0 | 0.0% |
| `synonym` | DrugBank migration | Raw synonym string | `Vitamin E`, `alpha-Tocopherol`, `Aspirin` | 0 | 0.0% |

### Statistics

| Metric | Value |
|--------|-------|
| Total synonym rows | 18,160 |
| Distinct ingredient IDs | 18,160 |
| Min synonyms per ingredient | 1 |
| Max synonyms per ingredient | 65 |
| Avg synonyms per ingredient | 2.87 |
| NULL `synonym` | 0 |

**Top 5 ingredients by synonym count:**

| name | synonym_count |
|---||---|
| Castor oil | 65 |
| Turmeric | 57 |
| Yellowfin tuna | 51 |
| Acacia | 44 |
| Senna leaf | 44 |

### Sample Rows

| id | synonym |
|---||---|
| c750ced7-4187-565a-aa74-d4d511966f33 | Uroquinasa |
| 95d85f85-264c-598e-abc8-88904831c14b | Leuporelin |
| 95d85f85-264c-598e-abc8-88904831c14b | Leuprorelin |
| 95d85f85-264c-598e-abc8-88904831c14b | Leuprorelina |
| 95d85f85-264c-598e-abc8-88904831c14b | Leuproreline |

### Expected vs Actual

| Expectation | Actual |
|------------|--------|
| All `id` values match `drugdb.ingredients.id` | 18,160 of 18,160 matched — 0 orphaned |
| No NULL synonyms | 0 NULL synonyms |

### Data Quality Issues

- **0 orphaned synonym IDs**: `id` values with no matching row in `drugdb.ingredients`. Likely DrugBank entries removed during migration.
- Synonym lookup in `second_pass_ingredient_mapping.py` excludes synonyms whose lowercase form already appears in `direct_lookup` to prevent false matches.

### Relationships

| Related Table | Join | Type |
|---------------|------|------|
| `drugdb.ingredients` | `id = id` | Many-to-one |

---

## 3. drugdb.indian_brand_ingredient

### Purpose
Maps Indian pharmaceutical brand ingredient names to DrugBank IDs. Pre-existing table updated by CDSS pipeline via multi-tier exact/fuzzy matching against `drugdb.ingredients` and `drugdb.ingredient_synonyms`.

### Schema

| Column | Type | Nullable | Default | PK | FK References |
|--------|------|----------|---------|----|----|
| `id` | integer | NO | nextval('drugdb.indian_bran... | ✓ |  |
| `indian_brand_id` | integer | NO |  |  | drugdb.indian_brand.indian_brand_id |
| `ingredient_index` | integer | NO |  |  |  |
| `ingredient_name_raw` | text | NO |  |  |  |
| `ingredient_name_norm` | text | NO |  |  |  |
| `strength_mass` | numeric | YES |  |  |  |
| `strength_unit` | text | YES |  |  |  |
| `strength_label` | text | YES |  |  |  |
| `rxcui_in` | text | YES |  |  |  |
| `match_confidence` | text | YES |  |  |  |
| `drugbank_id` | text | YES |  |  |  |

### Indexes & Constraints

- `indian_brand_ingredient_pkey`: `CREATE UNIQUE INDEX indian_brand_ingredient_pkey ON drugdb.indian_brand_ingredient USING btree (id)`
- `indian_brand_ingredient_indian_brand_id_ingredient_index_key`: `CREATE UNIQUE INDEX indian_brand_ingredient_indian_brand_id_ingredient_index_key ON drugdb.indian_brand_ingredient USING btree (indian_brand_id, ingredient_index)`
- `idx_ibi_brand_id`: `CREATE INDEX idx_ibi_brand_id ON drugdb.indian_brand_ingredient USING btree (indian_brand_id)`
- `idx_ibi_name_norm`: `CREATE INDEX idx_ibi_name_norm ON drugdb.indian_brand_ingredient USING btree (ingredient_name_norm)`
- `idx_ibi_name_trgm`: `CREATE INDEX idx_ibi_name_trgm ON drugdb.indian_brand_ingredient USING gin (ingredient_name_norm gin_trgm_ops)`
- `idx_ibi_rxcui`: `CREATE INDEX idx_ibi_rxcui ON drugdb.indian_brand_ingredient USING btree (rxcui_in)`
- `uq_brand_ing_idx`: `CREATE UNIQUE INDEX uq_brand_ing_idx ON drugdb.indian_brand_ingredient USING btree (indian_brand_id, ingredient_index)`

### Population Script
**`update_indian_brand_drugbank_id.py`** + **`bulk_update_drugbank_fast.py`** — Pass 1: 4-tier exact/prefix matching; 559,356 rows (96.33%).  
**`fuzzy_match_indian_ingredients.py`** + **`apply_fuzzy_matches.py`** — Pass 2: 5-tier fuzzy; +16,483 rows (+156 distinct ingredients). Total: **99.17%**.

### Statistics

| Metric | Value |
|--------|-------|
| Total rows | 580,669 |
| With `drugbank_id` | 575,839 (99.17%) |
| Missing `drugbank_id` | 4,830 (0.83%) |
| Distinct raw ingredient names | 1,877 |
| Distinct normalized names | 1,875 |
| Distinct DrugBank IDs assigned | 1,627 |

### Sample Rows

| id | indian_brand_id | ingredient_index | ingredient_name_raw | ingredient_name_norm | strength_mass | strength_unit | strength_label | rxcui_in | match_confidence | drugbank_id |
|---||---||---||---||---||---||---||---||---||---||---|
| 282588 | 174629 | 0 | Gabapentin | gabapentin | 400.0000 | mg | 400mg | NULL | NULL | DB00996 |
| 282617 | 174648 | 0 | Ibuprofen | ibuprofen | 100.0000 | mg | 100mg | NULL | NULL | DB01050 |
| 282556 | 174608 | 2 | Voglibose | voglibose | 0.2000 | mg | 0.2mg | NULL | NULL | DB04878 |
| 282608 | 174643 | 0 | Aripiprazole | aripiprazole | 30.0000 | mg | 30mg | NULL | NULL | DB01238 |
| 282593 | 174631 | 0 | Fluorometholone | fluorometholone | 0.1000 | % w/v | 0.1% w/v | NULL | NULL | DB00324 |

### Sample Unmapped Rows (drugbank_id IS NULL)

| id | indian_brand_id | ingredient_index | ingredient_name_raw | ingredient_name_norm | strength_mass | strength_unit | strength_label | rxcui_in | match_confidence | drugbank_id |
|---||---||---||---||---||---||---||---||---||---||---|
| 282547 | 174604 | 1 | Tricholine | tricholine | 275.0000 | mg | 275mg | NULL | NULL | NULL |
| 282599 | 174635 | 1 | Tricholine | tricholine | 27.5000 | mg | 27.5mg | NULL | NULL | NULL |
| 282684 | 174690 | 1 | Tricholine | tricholine | 275.0000 | mg | 275mg | NULL | NULL | NULL |

### Data Quality Issues

- **4,830 rows still NULL** (~129 distinct ingredients): `Tricholine`, `Divalproex`, `Lactic acid bacillus` etc. No reliable DrugBank match. Require manual mapping.
- Tier 5.5 (partial/substring fuzzy) was skipped — single-letter DrugBank entries produced false 100% scores.

### Relationships

| Related Table | Join | Type | Coverage |
|---------------|------|------|----------|
| `drugdb.ingredients` | `drugbank_id = drugbank_id` | Many-to-one | 1,627 / 1,627 distinct IDs matched |

---

## 4. public.DrugSourceMaster

### Purpose
Registry table tracking raw source records from upstream data providers (OpenFDA, RxNorm, DrugBank, DailyMed). Foundation from which `DrugMasterLinkage` is built.

### Schema

| Column | Type | Nullable | Default | PK | FK References |
|--------|------|----------|---------|----|----|
| `id` | uuid | NO |  | ✓ |  |
| `sourceid` | text | NO |  |  |  |
| `source` | text | NO | 'openfda'::text |  |  |
| `record` | jsonb | NO |  |  |  |
| `keyterms` | jsonb | NO |  |  |  |
| `clean_record` | jsonb | YES |  |  |  |
| `standardized_records` | jsonb | YES |  |  |  |

### Indexes & Constraints

- `DrugSourceMaster_pkey`: `CREATE UNIQUE INDEX "DrugSourceMaster_pkey" ON public."DrugSourceMaster" USING btree (id)`
- `idx_drug_source_master_sourceid`: `CREATE INDEX idx_drug_source_master_sourceid ON public."DrugSourceMaster" USING btree (sourceid)`
- `idx_drug_source_master_source_sourceid_unique`: `CREATE UNIQUE INDEX idx_drug_source_master_source_sourceid_unique ON public."DrugSourceMaster" USING btree (source, sourceid)`
- `idx_dsm_source_id`: `CREATE INDEX idx_dsm_source_id ON public."DrugSourceMaster" USING btree (source, id)`
- `idx_dsm_unii_drugbank`: `CREATE INDEX idx_dsm_unii_drugbank ON public."DrugSourceMaster" USING btree ((((record -> 'drug'::text) ->> 'unii'::text))) WHERE (source = 'drugbank'::text)`
- `idx_dsm_sourceid_drugbank`: `CREATE INDEX idx_dsm_sourceid_drugbank ON public."DrugSourceMaster" USING btree (sourceid) WHERE (source = 'drugbank'::text)`
- `idx_drugsourcemaster_drugbank_unii`: `CREATE INDEX idx_drugsourcemaster_drugbank_unii ON public."DrugSourceMaster" USING btree ((((record -> 'drug'::text) ->> 'unii'::text))) WHERE (source = 'drugbank'::text)`

### Statistics

**Total rows:** 738,197

**Rows by source:**

| Source | Row Count |
|--------|-----------|
| rxnorm | 410,459 |
| openfda | 256,165 |
| dailymed | 51,731 |
| drugbank | 19,842 |

### Sample Rows

| id | sourceid | source | record | keyterms | clean_record | standardized_records |
|---||---||---||---||---||---||---|
| 7c7c0ccc-4f48-579d-b5f3-2dad771d4804 | 3869e21c-4720-4c37-8e2d-97b0d8e89ac9 | openfda | {'id': '3869e21c-4720-4c37-8e2d-97b0d8e89ac9', 'set_... | {'brand_name': ['Halobetasol Propionate'], 'generic_... | {'set_id': 'c4e78fa5-6ced-4c90-89c6-31e5fc71b837', '... | {'safety': {'precautions': {'text': "PRECAUTIONS Gen... |
| 3791f9a0-dda6-4b9f-a76c-56dc48b259cb | 1921219 | rxnorm | {'rxcui': '1921219', 'entries': [{'sab': 'SNOMEDCT_U... | {'tty': ['FN', 'PT', 'SCDG'], 'name': 'Brigatinib-co... | NULL | {'rxcui': '1921219', 'entries': [{'sab': 'SNOMEDCT_U... |
| 37335588-a501-4d53-9b49-b93f4618fa7d | 1924965 | rxnorm | {'rxcui': '1924965', 'entries': [{'sab': 'MTHSPL', '... | {'tty': ['DP'], 'name': 'BETULA PAPYRIFERA POLLEN 0.... | NULL | {'rxcui': '1924965', 'entries': [{'sab': 'MTHSPL', '... |

### Relationships
Indirect — source records are aggregated into `DrugMasterLinkage` via the linkage pipeline.

---

## 5. public.DrugMasterLinkage

### Purpose
Central aggregation table. Each row is a unique drug entity linked across multiple sources. `combined_clean_jsonb` contains fully merged data and is the primary input for all downstream population scripts.

### Schema

| Column | Type | Nullable | Default | PK | FK References |
|--------|------|----------|---------|----|----|
| `master_linkage_id` | uuid | NO |  | ✓ |  |
| `set_ids` | ARRAY | NO | '{}'::text[] |  |  |
| `openfda_master_ids` | ARRAY | NO | '{}'::uuid[] |  |  |
| `dailymed_master_ids` | ARRAY | NO | '{}'::uuid[] |  |  |
| `drugbank_master_ids` | ARRAY | NO | '{}'::uuid[] |  |  |
| `drugbank_ids` | ARRAY | NO | '{}'::text[] |  |  |
| `rxcui_ids` | ARRAY | NO | '{}'::text[] |  |  |
| `unii_ids` | ARRAY | NO | '{}'::text[] |  |  |
| `relation_openfda_dailymed` | text | NO |  |  |  |
| `relation_rxnorm_drugbank` | text | NO |  |  |  |
| `link_sources` | ARRAY | NO | '{}'::text[] |  |  |
| `confidence` | text | NO |  |  |  |
| `evidence_summary` | jsonb | NO | '{}'::jsonb |  |  |
| `created_at` | timestamp with time zone | NO | now() |  |  |
| `updated_at` | timestamp with time zone | NO | now() |  |  |
| `combined_clean_jsonb` | jsonb | YES |  |  |  |
| `standard_combined_json` | jsonb | YES |  |  |  |

### Indexes & Constraints

- `MasterLinkage_pkey`: `CREATE UNIQUE INDEX "MasterLinkage_pkey" ON public."DrugMasterLinkage" USING btree (master_linkage_id)`
- `idx_master_linkage_set_ids_gin`: `CREATE INDEX idx_master_linkage_set_ids_gin ON public."DrugMasterLinkage" USING gin (set_ids)`
- `idx_master_linkage_drugbank_ids_gin`: `CREATE INDEX idx_master_linkage_drugbank_ids_gin ON public."DrugMasterLinkage" USING gin (drugbank_ids)`
- `idx_master_linkage_confidence`: `CREATE INDEX idx_master_linkage_confidence ON public."DrugMasterLinkage" USING btree (confidence)`
- `idx_dml_unii_ids_gin`: `CREATE INDEX idx_dml_unii_ids_gin ON public."DrugMasterLinkage" USING gin (unii_ids)`
- `idx_dml_null_jsonb`: `CREATE INDEX idx_dml_null_jsonb ON public."DrugMasterLinkage" USING btree (master_linkage_id) WHERE (combined_clean_jsonb IS NULL)`
- `idx_dml_updated_at`: `CREATE INDEX idx_dml_updated_at ON public."DrugMasterLinkage" USING btree (updated_at, master_linkage_id)`
- `idx_dml_null_jsonb_updated`: `CREATE INDEX idx_dml_null_jsonb_updated ON public."DrugMasterLinkage" USING btree (updated_at, master_linkage_id) WHERE (combined_clean_jsonb IS NULL)`

### JSONB Top-Level Key Distribution

| JSONB Key | Present in (rows) | Description |
|-----------|------------------|-------------|
| `dailymed` | 50,111 | Source-specific block |
| `drugbank` | 50,111 | Source-specific block |
| `openfda` | 50,111 | Source-specific block |
| `rxnorm` | 50,111 | Source-specific block |

### Statistics

| Metric | Value |
|--------|-------|
| Total rows | 50,111 |
| With `combined_clean_jsonb` | 50,111 (100.0%) |
| With `rxnorm` key in JSONB | 50,111 |
| With non-empty `rxnorm` array | 47,619 |
| With `openfda` key | 50,111 |
| With `drugbank` key | 50,111 |
| NULL `combined_clean_jsonb` | 0 |
| With `standard_combined_json` | 0 |
| Linkage records → `drugdb.drug` | 47,619 / 50,111 (95.0%) |
| Not represented in `drugdb.drug` | 2,492 |

**Confidence distribution:**

| Confidence | Count |
|-----------|-------|
| high | 50,110 |
| medium | 1 |

### Sample Rows (metadata only, no raw JSONB)

| master_linkage_id | confidence | set_id_count | has_rxnorm | has_openfda | has_drugbank |
|---||---||---||---||---||---|
| c330730b-6d44-557b-9da4-7bba1a4afd1f | high | 1 | True | True | True |
| d68479d3-c018-57c3-8dfc-8826ac00fccb | high | 1 | True | True | True |
| 8a540e63-3f3f-523b-ac57-18f321c3372f | high | 1 | True | True | True |
| 5d9fbf36-cd6b-511c-9fe8-e5fe1a3f6720 | high | 1 | True | True | True |
| 524ac8dc-0318-5406-a0a7-035cd01b4527 | high | 1 | True | True | True |

### Data Quality Issues

- **0 rows with NULL `combined_clean_jsonb`**: not yet through the cleaning pipeline. Downstream scripts skip gracefully.
- **2,492 linkage records not in `drugdb.drug`**: no rxnorm entries or NULL JSONB. Expected.

### Relationships

| Related Table | Join Column | Type | Coverage |
|---------------|-------------|------|----------|
| `drugdb.drug` | `master_linkage_id` | One-to-many | 95.0% of linkage records → drug rows |

---

## 6. drugdb.drug

### Purpose
Central formulation table. One row per unique drug formulation derived from `rxnorm[]` entries in `DrugMasterLinkage.combined_clean_jsonb`. Primary hub for `drug_ingredient_mapping` and `drug_synonym_formulation`.

### Schema

| Column | Type | Nullable | Default | PK | FK References |
|--------|------|----------|---------|----|----|
| `formulation_id` | uuid | NO |  | ✓ |  |
| `master_linkage_id` | uuid | YES |  |  | public.DrugMasterLinkage.master_linkage_id |
| `generic_name` | text | YES |  |  |  |
| `generic_formulation` | text | YES |  |  |  |
| `dosage_forms` | text | YES |  |  |  |
| `generic_formulation_original` | text | YES |  |  |  |
| `rxnorm_generic_formulation` | text | YES |  |  |  |
| `rxcui` | character varying(50) | YES |  |  |  |

### Indexes & Constraints

- `drug_pkey`: `CREATE UNIQUE INDEX drug_pkey ON drugdb.drug USING btree (formulation_id)`
- `idx_drug_master_linkage_id`: `CREATE INDEX idx_drug_master_linkage_id ON drugdb.drug USING btree (master_linkage_id)`
- `idx_drug_generic_name`: `CREATE INDEX idx_drug_generic_name ON drugdb.drug USING btree (generic_name)`
- `idx_drug_formulation_dosage`: `CREATE INDEX idx_drug_formulation_dosage ON drugdb.drug USING btree (generic_formulation, dosage_forms)`
- `idx_drug_rxnorm_formulation`: `CREATE INDEX idx_drug_rxnorm_formulation ON drugdb.drug USING btree (rxnorm_generic_formulation)`
- `idx_drug_rxcui`: `CREATE INDEX idx_drug_rxcui ON drugdb.drug USING btree (rxcui)`

### Data Source & Derivation

| Column | JSONB Path | Transformation |
|--------|-----------|----------------|
| `formulation_id` | — | `uuid5(NAMESPACE_OID, "{master_linkage_id}|{cleaned_formulation}|{dosage_form}")` — deterministic |
| `master_linkage_id` | `DrugMasterLinkage.master_linkage_id` | Direct copy |
| `generic_name` | `combined_clean_jsonb → openfda → drug_info → generic_name` | None |
| `generic_formulation` | `combined_clean_jsonb → rxnorm[] → generic_formulation` | Dosage-form suffix stripped |
| `dosage_forms` | `combined_clean_jsonb → rxnorm[] → specific_dosage_form` | None |
| `rxnorm_generic_formulation` | `combined_clean_jsonb → rxnorm[] → generic_formulation` | Raw — no suffix strip |
| `rxcui` | `combined_clean_jsonb → rxnorm[] → rxcui` | None |
| `generic_formulation_original` | Same as `generic_formulation` | Pre-cleanup backup |

### Population Script
**`populate_drug_table.py`** — Streams `DrugMasterLinkage` via server-side cursor; extracts every `rxnorm[]` entry; deterministic UUID5 `formulation_id`. Two-connection pattern (read/write).  
**`update_drug_rxnorm_columns.py`** + **`fix_rxnorm_uncleaned_rows.py`** — Backfill `rxcui` and `rxnorm_generic_formulation` on all 88,983 rows.  
**One-time cleanup migration** — Corrected 17,128 rows where suffix stripping failed on EU/uppercase dosage form codes.

### Column-Level Mapping

| Column | Source | Example Values | NULL Count | NULL % |
|--------|--------|----------------|-----------|--------|
| `formulation_id` | Computed UUID5 | `3f2a1b...`, `9c4e7d...` | 0 | 0% |
| `master_linkage_id` | `DrugMasterLinkage.master_linkage_id` | UUID | 0 | 0% |
| `generic_name` | `openfda.drug_info.generic_name` | `aspirin`, `metformin hydrochloride` | 0 | 0.0% |
| `generic_formulation` | `rxnorm[].generic_formulation` (cleaned) | `aspirin 81 MG`, `quetiapine 100 MG` | 0 | 0.0% |
| `dosage_forms` | `rxnorm[].specific_dosage_form` | `Oral Tablet`, `Injectable Solution` | 0 | 0.0% |
| `rxnorm_generic_formulation` | `rxnorm[].generic_formulation` (raw) | `aspirin 81 MG Oral Tablet` | 0 | 0.0% |
| `rxcui` | `rxnorm[].rxcui` | `1191`, `860975`, `312743` | 0 | 0.0% |
| `generic_formulation_original` | Pre-cleanup backup | Same as `generic_formulation` | 0 | 0.0% |

### Statistics

| Metric | Value |
|--------|-------|
| Total rows | 88,983 |
| With `generic_name` | 88,983 (100.0%) |
| Missing `generic_name` | 0 (0.0%) |
| With `rxcui` (100% target) | 88,983 (100.0%) |
| Distinct `master_linkage_id` values | 47,619 |
| Distinct `rxcui` values | 8,074 |
| Distinct `dosage_forms` | 150 |
| Distinct `generic_name` | 4,849 |
| `generic_formulation` min / max / avg length | 11 / 524 / 27.4 chars |
| `generic_name` min / max / avg length | 0 / 268 / 18.1 chars |

**Top 15 dosage forms:**

| Dosage Form | Count |
|-------------|-------|
| Oral Tablet | 34,672 |
| Oral Capsule | 8,123 |
| Injection | 5,903 |
| TABLET, EXTENDED RELEASE | 3,725 |
| Extended Release Oral Tablet | 3,725 |
| SOLUTION FOR INJECTION | 3,174 |
| Injectable Solution | 3,174 |
| CAPSULE, EXTENDED RELEASE | 1,567 |
| Extended Release Oral Capsule | 1,567 |
| INHALATION GAS | 1,370 |
| Gas for Inhalation | 1,370 |
| Oral Solution | 1,201 |
| SOLUTION, ORAL | 1,201 |
| Topical Cream | 1,183 |
| Oral Suspension | 923 |

### Sample Rows

| formulation_id | master_linkage_id | generic_name | generic_formulation | dosage_forms | rxcui |
|---||---||---||---||---||---|
| c973975a-5c1e-5a9c-935d-eeaa973829cd | 111d45ed-d3ba-5cba-b922-657c87f6d816 | ALPRAZOLAM | alprazolam 0.25 MG | Oral Tablet | 308047 |
| 980a5129-4a6e-57ca-834a-e4790c89a2cc | 111d45ed-d3ba-5cba-b922-657c87f6d816 | ALPRAZOLAM | alprazolam 0.5 MG | Oral Tablet | 308048 |
| a275914e-e739-50d8-8405-009ae9e8b9b2 | 1c5d6d55-c845-5003-9117-aa1794b4ad15 | FENOFIBRATE | fenofibrate 160 MG | Oral Tablet | 349287 |
| 9f6a03d5-fe8b-557e-8545-fb972972813d | 1c5d6d55-c845-5003-9117-aa1794b4ad15 | FENOFIBRATE | fenofibrate 54 MG | Oral Tablet | 351133 |
| cd4ea547-54c3-51b9-b001-dd4b544ecc2c | 21c2e3f1-9738-5ee9-8d32-7772ef2aae8d | CLOPIDOGREL | clopidogrel 75 MG | Oral Tablet | 309362 |

### Expected vs Actual

| Expectation | Actual | Gap |
|------------|--------|-----|
| 88,983 rows from 50,111 linkage records | 88,983 | On target |
| 100% `rxcui` coverage | 88,983 / 88,983 | 0 missing |
| ~6.4% missing `generic_name` is expected | 0 (0.0%) | Expected |

### Data Quality Issues

- **0 rows missing `generic_name`**: some OpenFDA entries lack this field. Not fixable without a different data source.
- **17,128 rows** (fixed 2026-05-02): dosage form suffixes incorrectly embedded in `generic_formulation`. Root cause: `strip_dosage_form_suffix()` only did exact matching; EU/uppercase codes like `TABLET, EXTENDED RELEASE` failed.
- **`generic_formulation_original`** backup column retained for audit/rollback.

### Relationships

| Related Table | Join Column | Type | Coverage |
|---------------|-------------|------|----------|
| `DrugMasterLinkage` | `master_linkage_id` | Many-to-one | 47,619 / 50,111 linkage records |
| `drug_ingredient_mapping` | `formulation_id` | One-to-many | 99.84% of formulations |
| `drug_synonym_formulation` | `formulation_id` | One-to-one (UNIQUE) | 74.34% of formulations |

---

## 7. drugdb.drug_ingredient_mapping

### Purpose
Maps each drug formulation to its constituent ingredients with dosage strength (mass + unit). One row per `(formulation, ingredient)` pair. Multi-ingredient formulations produce multiple rows. Composite PK enforces no duplicates.

### Schema

| Column | Type | Nullable | Default | PK | FK References |
|--------|------|----------|---------|----|----|
| `formulation_id` | uuid | NO |  | ✓ | drugdb.drug.formulation_id |
| `ingredient_id` | uuid | NO |  | ✓ | drugdb.ingredients.id |
| `mass` | numeric | YES |  |  |  |
| `unit` | character varying(50) | YES |  |  |  |

### Indexes & Constraints

- `drug_ingredient_mapping_pkey`: `CREATE UNIQUE INDEX drug_ingredient_mapping_pkey ON drugdb.drug_ingredient_mapping USING btree (formulation_id, ingredient_id)`
- `idx_dim_formulation_id`: `CREATE INDEX idx_dim_formulation_id ON drugdb.drug_ingredient_mapping USING btree (formulation_id)`
- `idx_dim_ingredient_id`: `CREATE INDEX idx_dim_ingredient_id ON drugdb.drug_ingredient_mapping USING btree (ingredient_id)`

### Data Source & Derivation

| Column | JSONB Path | Transformation |
|--------|-----------|----------------|
| `formulation_id` | `rxnorm[].rxcui` → lookup in `drugdb.drug` | O(1) dict lookup by rxcui |
| `ingredient_id` | `rxnorm[].ingredients[].name` → lookup in `drugdb.ingredients` | Case-insensitive name match |
| `mass` | `rxnorm[].ingredients[].scdc.mass` | Cast to NUMERIC |
| `unit` | `rxnorm[].ingredients[].scdc.unit` | None |

### Population Scripts
**Pass 1 — `populate_drug_ingredient_mapping.py`**: direct case-insensitive name match vs `drugdb.ingredients.name`. Achieved 94.76% coverage (84,316 formulations, 92,570 rows).  
**Pass 2 — `second_pass_ingredient_mapping.py`**: synonym fallback via `drugdb.ingredient_synonyms`. Recovered 264 distinct ingredient names → 5,262 additional rows. **Final: 98,832 rows, 99.84% coverage**.

### Column-Level Mapping

| Column | Source | Example Values | NULL Count | NULL % |
|--------|--------|----------------|-----------|--------|
| `formulation_id` | `drugdb.drug.formulation_id` via rxcui lookup | UUID | 0 | 0% |
| `ingredient_id` | `drugdb.ingredients.id` via name match | UUID | 0 | 0% |
| `mass` | `rxnorm[].ingredients[].scdc.mass` | `100`, `325`, `0.25` | 0 | 0.0% |
| `unit` | `rxnorm[].ingredients[].scdc.unit` | `MG`, `G`, `MEQ` | 0 | 0.0% |

### Statistics

| Metric | Value |
|--------|-------|
| Total rows | 98,832 |
| Distinct formulations covered | 88,839 of 88,983 (99.84%) |
| Uncovered formulations | 144 |
| Distinct ingredients used | 2,107 |
| With `mass` | 98,832 (100.0%) |
| Missing `mass` | 0 |
| With `unit` | 98,832 (100.0%) |
| Missing `unit` | 0 |
| Min / Max / Avg mass | 0.000003 / 40000000000000.0 / 1308960996.3294 |
| Multi-ingredient formulations | 7,784 |
| Max / Avg ingredients per formulation | 21 / 1.11 |

**Ingredient quality (join to `drugdb.ingredients`):**

| Metric | Value |
|--------|-------|
| Total mapping rows | 98,832 |
| Distinct `ingredient_id` values | 2,107 |
| Ingredient IDs matched in `drugdb.ingredients` | 2,107 |
| Mapping rows where ingredient has `drugbank_id` | 96,817 |
| Mapping rows where ingredient has `rxcui` | 97,526 |

**Unit distribution:**

| Unit | Count |
|------|-------|
| MG | 66,366 |
| MG/ML | 22,471 |
| % | 2,942 |
| MG/MG | 2,090 |
| UNT/ML | 1,545 |
| MG/HR | 809 |
| MEQ | 701 |
| MEQ/ML | 658 |
| MG/ACTUAT | 645 |
| UNT/MG | 265 |

### Sample Rows (with joined names)

| formulation_id | generic_formulation | ingredient_name | drugbank_id | mass | unit |
|---||---||---||---||---||---|
| c973975a-5c1e-5a9c-935d-eeaa973829cd | alprazolam 0.25 MG | Alprazolam | DB00404 | 0.25 | MG |
| 980a5129-4a6e-57ca-834a-e4790c89a2cc | alprazolam 0.5 MG | Alprazolam | DB00404 | 0.5 | MG |
| a275914e-e739-50d8-8405-009ae9e8b9b2 | fenofibrate 160 MG | Fenofibrate | DB01039 | 160.0 | MG |
| 9f6a03d5-fe8b-557e-8545-fb972972813d | fenofibrate 54 MG | Fenofibrate | DB01039 | 54.0 | MG |
| cd4ea547-54c3-51b9-b001-dd4b544ecc2c | clopidogrel 75 MG | Clopidogrel | DB00758 | 75.0 | MG |

### Data Quality Issues

- **144 uncovered formulations**: no `ingredients[]` array in their RxNorm entry — combination products or unavailable data. Expected.
- **0 NULL mass** / **0 NULL unit**: some RxNorm `scdc` objects incomplete.
- Composite PK `(formulation_id, ingredient_id)` prevents any duplicates at DB level.

### Relationships

| Related Table | Join | Type | Coverage |
|---------------|------|------|----------|
| `drugdb.drug` | `formulation_id` | Many-to-one | 99.84% of formulations covered |
| `drugdb.ingredients` | `ingredient_id = id` | Many-to-one | 2,107 of 2,107 IDs matched |

---

## 8. drugdb.drug_synonym_formulation

### Purpose
Stores all RxNorm synonyms for each drug formulation as a native PostgreSQL `TEXT[]` array. One row per formulation (UNIQUE on `formulation_id`). Used for search/discovery of equivalent drug names.

### Schema

| Column | Type | Nullable | Default | PK | FK References |
|--------|------|----------|---------|----|----|
| `id` | integer | NO | nextval('drug_synonym_formu... | ✓ |  |
| `formulation_id` | uuid | YES |  |  | drugdb.drug.formulation_id |
| `synonyms` | ARRAY | YES |  |  |  |

### Indexes & Constraints

- `drug_synonym_formulation_pkey`: `CREATE UNIQUE INDEX drug_synonym_formulation_pkey ON drugdb.drug_synonym_formulation USING btree (id)`
- `uq_drug_synonym_formulation_formulation_id`: `CREATE UNIQUE INDEX uq_drug_synonym_formulation_formulation_id ON drugdb.drug_synonym_formulation USING btree (formulation_id)`
- `idx_drug_synonym_formulation_formulation_id`: `CREATE INDEX idx_drug_synonym_formulation_formulation_id ON drugdb.drug_synonym_formulation USING btree (formulation_id)`
- `idx_dsf_synonyms_gin`: `CREATE INDEX idx_dsf_synonyms_gin ON drugdb.drug_synonym_formulation USING gin (synonyms)`

### Data Source & Derivation

| Column | JSONB Path | Transformation |
|--------|-----------|----------------|
| `formulation_id` | `rxnorm[].rxcui` → lookup in `drugdb.drug` | O(1) dict lookup by rxcui |
| `synonyms` | `rxnorm[].synonyms[]` | Parsed from JSON array or Python-repr string; stored as `TEXT[]` |

### Population Script
**`populate_drug_synonym_formulation.py`** — Loads `(rxcui → formulation_id)` dict at startup. For each `rxnorm[]` entry looks up `formulation_id` by rxcui. Handles two synonym formats (proper JSON array and Python-repr string). Resume-safe via `ON CONFLICT DO NOTHING`.

### Statistics

| Metric | Value |
|--------|-------|
| Total rows | 66,154 |
| Distinct formulations covered | 66,154 of 88,983 (74.34%) |
| Formulations without synonyms | 22,829 |
| Min / Max / Avg synonyms per row | 1 / 48 / 2.28 |
| NULL `synonyms` arrays | 0 |

### Sample Rows (highest synonym counts)

| formulation_id | generic_formulation | synonym_count | first_2_synonyms |
|---||---||---||---|
| c4e37197-652b-5222-a5a8-6267b5187ce8 | ascorbic acid 30 MG / cholecalciferol 1000 UNT / cup... | 48 | ['vit-C 30 MG / cholecalciferol 1000 UNT / cuprous o... |
| 094fa8d1-14d5-54d6-91d7-78032cfeb8c0 | ascorbic acid 100 MG / biotin 0.15 MG / calcium pant... | 25 | ['ascorbic acid 100 MG / biotin 0.15 MG / calcium pa... |
| f3773d83-0cf3-5c9e-9316-3dfd825c7e42 | ascorbic acid 100 MG / biotin 0.3 MG / calcium carbo... | 24 | ['vit-C 100 MG / biotin 0.3 MG / calcium carbonate 1... |
| 046a7124-14fc-5eb3-8556-25103d92cfa3 | ascorbic acid 210 MG / biotin 0.3 MG / calcium panto... | 24 | ['vit-C 210 MG / biotin 0.3 MG / calcium pantothenat... |
| 0ee87e35-3247-543a-bfcd-0b14d2e501b7 | ascorbic acid 100 MG / biotin 0.3 MG / calcium panto... | 24 | ['vit-C 100 MG / biotin 0.3 MG / calcium pantothenat... |

### Data Quality Issues

- **22,829 formulations uncovered**: no synonym data in source JSONB. Expected — not a pipeline error.
- Two synonym string formats in source handled transparently by the population script.

### Relationships

| Related Table | Join | Type | Coverage |
|---------------|------|------|----------|
| `drugdb.drug` | `formulation_id` | Many-to-one (UNIQUE) | 74.34% of formulations |

---

## 9. Entity Relationship Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│              public.DrugSourceMaster                            │
│  id  PK │ source │ record │ clean_record                       │
└─────────────────────────────┬────────────────────────────────────┘
                              │ (indirect via linkage pipeline)
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│              public.DrugMasterLinkage                           │
│  master_linkage_id  UUID  PK                                    │
│  combined_clean_jsonb  JSONB  ◄── central merged data store     │
│    ├─ rxnorm[].rxcui                                           │
│    ├─ rxnorm[].generic_formulation                             │
│    ├─ rxnorm[].specific_dosage_form                            │
│    ├─ rxnorm[].synonyms[]                                      │
│    ├─ rxnorm[].ingredients[].name                              │
│    ├─ rxnorm[].ingredients[].scdc.{mass, unit}                 │
│    └─ openfda.drug_info.generic_name                           │
└─────────────────────────────┬────────────────────────────────────┘
                              │ master_linkage_id  (1 : N)
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                    drugdb.drug                                  │
│  formulation_id  UUID  PK  (deterministic UUID5)                │
│  master_linkage_id  UUID  FK ──────────────────────────────────┘│
│  generic_name    TEXT                                           │
│  generic_formulation  TEXT    (suffix-stripped)                 │
│  rxnorm_generic_formulation  TEXT  (raw)                        │
│  dosage_forms    TEXT                                           │
│  rxcui           VARCHAR(50)                                    │
│  generic_formulation_original  TEXT  (backup)                   │
└─────────┬─────────────────────────────┬───────────────────────-─┘
          │ formulation_id (1 : N)       │ formulation_id (1 : 1 UNIQUE)
          ▼                              ▼
┌──────────────────────┐    ┌────────────────────────────────────┐
│ drug_ingredient_     │    │   drug_synonym_formulation         │
│ mapping              │    │  id          SERIAL  PK            │
│  formulation_id UUID │    │  formulation_id UUID  FK  UNIQUE   │
│  ingredient_id  UUID │    │  synonyms    TEXT[]                │
│  mass    NUMERIC     │    └────────────────────────────────────┘
│  unit    VARCHAR(50) │
│  PK(formulation_id,  │
│     ingredient_id)   │
└──────────┬───────────┘
           │ ingredient_id (N : 1)
           ▼
┌──────────────────────────────────────────────────────────┐
│                  drugdb.ingredients                      │
│  id          UUID  PK                                    │
│  name        VARCHAR                                     │
│  drugbank_id VARCHAR                                     │
│  rxcui       VARCHAR                                     │
│  unii        VARCHAR                                     │
│  indications TEXT                                        │
│  general_function TEXT                                   │
│  pharmacodynamics TEXT                                   │
│  classification_description TEXT                         │
│  food_interactions JSONB                                 │
└──────────────┬─────────────────────────┬─────────────────┘
               │ id (1 : N)              │ drugbank_id (1 : N)
               ▼                         ▼
┌──────────────────────┐  ┌──────────────────────────────────────┐
│ ingredient_synonyms  │  │   indian_brand_ingredient            │
│  id      UUID        │  │  ingredient_name_raw  VARCHAR        │
│  synonym VARCHAR     │  │  ingredient_name_norm VARCHAR        │
└──────────────────────┘  │  drugbank_id  VARCHAR  (→ ing.db_id) │
                           └──────────────────────────────────────┘
```

**Relationship Summary:**

| From | To | Join Columns | Cardinality | Coverage |
|------|-----|-------------|-------------|----------|
| `DrugMasterLinkage` | `drug` | `master_linkage_id` | 1:N | 95.0% of linkage records |
| `drug` | `drug_ingredient_mapping` | `formulation_id` | 1:N | 99.84% of formulations |
| `drug` | `drug_synonym_formulation` | `formulation_id` | 1:1 (UNIQUE) | 74.34% of formulations |
| `drug_ingredient_mapping` | `drugdb.ingredients` | `ingredient_id = id` | N:1 | 2,107 of 2,107 IDs |
| `drugdb.ingredients` | `ingredient_synonyms` | `id = id` | 1:N | 18,160 / 18,160 IDs matched |
| `drugdb.ingredients` | `indian_brand_ingredient` | `drugbank_id` | 1:N | 1,627 / 1,627 IDs |

---

## 10. Cross-Table Coverage Summary

| Metric | Value |
|--------|-------|
| Total `DrugMasterLinkage` records | 50,111 |
| Linkage records → `drug` | 47,619 (95.0%) |
| Linkage records **not** in `drug` | 2,492 |
| Total formulations in `drug` | 88,983 |
| Formulations with ingredient mapping | 88,839 (99.84%) |
| Formulations with synonyms | 66,154 (74.34%) |
| Formulations with **both** | 66,018 |
| Formulations with **neither** | 8 |
| Total ingredient mapping rows | 98,832 |
| Distinct ingredients used in mapping | 2,107 |
| Mapping rows where ingredient has `drugbank_id` | 96,817 |
| Mapping rows where ingredient has `rxcui` | 97,526 |
| Indian brand rows with `drugbank_id` | 575,839 / 580,669 (99.17%) |
| Synonym IDs matching `ingredients` table | 18,160 / 18,160 |

---

## 11. Known Data Quality Issues

| Severity | Table | Issue | Count | Root Cause | Status |
|----------|-------|-------|-------|-----------|--------|
| Medium | `drugdb.ingredients` | Skeleton rows (rxcui ✓, drugbank_id ✗) | 190 | RxNorm-only ingredients not in DrugBank | 5 resolved 2026-05-04; 185 remain |
| Low | `drugdb.ingredients` | Rows with neither rxcui nor drugbank_id | 0 | Experimental/import artifacts | Expected |
| Low | `drugdb.ingredient_synonyms` | Orphaned synonym IDs | 0 | DrugBank entries removed post-migration | Not fixable without re-migration |
| Medium | `drugdb.indian_brand_ingredient` | Unmapped rows (drugbank_id NULL) | 4,830 | ~129 distinct names with no DrugBank match | Requires manual mapping |
| Low | `public.DrugMasterLinkage` | NULL `combined_clean_jsonb` | 0 | Records not through cleaning pipeline | Downstream scripts skip gracefully |
| Low | `public.DrugMasterLinkage` | Linkage records not in `drug` | 2,492 | No rxnorm entries or NULL JSONB | Expected |
| Low | `drugdb.drug` | Missing `generic_name` | 0 | OpenFDA entries without generic name field | Expected; no fix available |
| Fixed | `drugdb.drug` | Dosage form suffix in `generic_formulation` | 17,128 | `strip_dosage_form_suffix()` missed EU codes | Fixed via migration 2026-05-02 |
| Low | `drugdb.drug_ingredient_mapping` | Uncovered formulations | 144 | No `ingredients[]` in RxNorm source | Expected; combination products |
| Low | `drugdb.drug_ingredient_mapping` | NULL mass / unit | 0 / 0 | RxNorm `scdc` data incomplete in source | Expected |
| Low | `drugdb.drug_synonym_formulation` | Formulations without synonyms | 22,829 | No synonyms in RxNorm source | Expected |

---

## 12. Scripts Reference Summary

| Script | Target Table | What It Does | Status |
|--------|-------------|-------------|--------|
| `drugdb_migration.sql` | `drugdb.*` | Initial DrugBank data import (ingredients, synonyms, indian_brand) | ✅ Completed |
| `update_ingredient_rxcui.py` | `drugdb.ingredients` | Fills `rxcui` from RxNorm JSONB; inserts skeleton rows for new RxNorm-only ingredients | ✅ Completed 2026-05-02 |
| `populate_drug_table.py` | `drugdb.drug` | Extracts rxnorm[] from DrugMasterLinkage; inserts one row per formulation; deterministic UUID5 | ✅ Completed 2026-05-02 |
| `update_drug_rxnorm_columns.py` | `drugdb.drug` | Backfills `rxcui` + `rxnorm_generic_formulation` on all 88,983 rows | ✅ Completed 2026-05-02 |
| `fix_rxnorm_uncleaned_rows.py` | `drugdb.drug` | Patches 17,128 rows whose UUID was seeded from pre-cleanup formulation strings | ✅ Completed 2026-05-02 |
| `execute_stage1_updates.py` | `drugdb.drug` | One-time cleanup: corrects `generic_formulation` suffix for 17,128 EU-coded rows | ✅ Completed 2026-05-02 |
| `populate_drug_synonym_formulation.py` | `drugdb.drug_synonym_formulation` | Loads synonyms from JSONB; rxcui-based lookup; handles dual synonym formats; resume-safe | ✅ Completed 2026-05-02 |
| `populate_drug_ingredient_mapping.py` | `drugdb.drug_ingredient_mapping` | Pass 1: case-insensitive name match; 94.76% coverage (92,570 rows) | ✅ Completed 2026-05-02 |
| `second_pass_ingredient_mapping.py` | `drugdb.drug_ingredient_mapping` | Pass 2: synonym fallback; +264 ingredient names; 99.84% total coverage (98,832 rows) | ✅ Completed 2026-05-04 |
| `update_indian_brand_drugbank_id.py` | `drugdb.indian_brand_ingredient` | Phase 1 dry-run: 4-tier exact/prefix matching for drugbank_id | ✅ Completed 2026-05-01 |
| `bulk_update_drugbank_fast.py` | `drugdb.indian_brand_ingredient` | Bulk apply Phase 1 results via temp table UPDATE; 559,356 rows in ~15 min | ✅ Completed 2026-05-01 |
| `fuzzy_match_indian_ingredients.py` | `drugdb.indian_brand_ingredient` (analysis) | 5-tier fuzzy analysis on remaining 285 unmapped ingredients | ✅ Completed 2026-05-02 |
| `apply_fuzzy_matches.py` | `drugdb.indian_brand_ingredient` | Applies Tiers 5.1–5.4 matches; +16,483 rows; 99.17% total coverage | ✅ Completed 2026-05-02 |
| `fuzzy_ingredient_dedup.py` | `drugdb.ingredients` | Fuzzy dedup: 195 skeleton rows vs DrugBank rows; Phase 2 fills `drugbank_id` on 5 skeleton rows | ✅ Completed 2026-05-04 |


---
_Report generated by CDSS deep research analysis — 2026-05-04 07:32:29 UTC_