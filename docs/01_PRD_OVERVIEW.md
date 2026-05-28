# CDSS — Clinical Decision Support System
# Product Requirements Document (PRD)

**Version**: 1.0  
**Date**: April 2026  
**Status**: Implementation-Ready  
**Stack**: FastAPI · PostgreSQL + pgvector · Neo4j · vLLM (local) · Docker Compose

---

## 1. PRODUCT OVERVIEW

### 1.1 Purpose

Build a Clinical Decision Support System that helps Indian medical practitioners
make evidence-based prescribing decisions. The system is UI-driven: clinicians
interact through 9 fixed query templates, each backed by a deterministic
retrieval pipeline with LLM-composed responses.

### 1.2 Architecture Principle

```
DETERMINISTIC RETRIEVAL FIRST → LLM COMPOSITION LAST

Every clinical fact is retrieved via structured SQL / graph queries.
The LLM composes the natural-language response with citations.
The LLM NEVER computes doses, classifies severity, or overrides structured data.
```

### 1.3 The 9 Clinical Query Templates

| ID | Question | Primary Data Source | Computation |
|----|----------|---------------------|-------------|
| Q1 | Which medications treat disorder X? | Postgres (indications) | SQL filter + vector evidence |
| Q2 | Do these drugs interact? | Postgres (interactions) + Neo4j (pathways) | Pairwise SQL + graph traversal |
| Q3 | What are alternatives to drug X? | Postgres (drug_class + interactions) | SQL filter + exclusion |
| Q4 | What dose for this patient? | Postgres (dosing_regimens) | SQL filter + arithmetic |
| Q5 | Is drug X approved for this population? | Postgres (population_approval) | SQL lookup + vector evidence |
| Q6 | Safe drugs for condition Y given current meds? | Postgres (indications + interactions) | Q1 + Q2 composed |
| Q7 | Dose adjustment for renal/hepatic impairment? | Postgres (dosing_regimens) | SQL filter |
| Q8 | Food/timing/separation requirements? | Postgres (administration_timing) | SQL lookup |
| Q9 | Which strength minimizes pill burden? | Postgres (available_strengths) | Arithmetic only |

### 1.4 Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| API Framework | FastAPI (Python 3.11+) | REST endpoints for 9 queries |
| Primary Database | PostgreSQL 16 + pgvector | Structured facts + vector search |
| Graph Database | Neo4j 5.x | Drug interaction pathways, alternatives |
| LLM Serving | vLLM | Local model inference |
| Extraction Model | Qwen2.5-72B-Instruct-AWQ | Pass 2 structured extraction (batch) |
| Composition Model | Qwen2.5-7B-Instruct | Query-time response generation (real-time) |
| Classification Model | Qwen2.5-7B-Instruct | Interaction severity, drug class (batch) |
| Embedding Model | BAAI/bge-large-en-v1.5 | Chunk embedding for vector search |
| Container Orchestration | Docker Compose | Single-server deployment |
| Indian Brand Data | JSON file → Postgres | Brand-to-formulation mapping |

### 1.5 LLM Model Recommendation (Local Hosting)

```
BATCH INGESTION (Pass 2 — runs once, then monthly):
  Tier 1 (complex extraction): Qwen2.5-72B-Instruct-AWQ
    - Indication extraction, dosing regimen parsing, population approval
    - Requires: 2× A100 80GB or 4× A6000 48GB
    - Throughput: ~800 tok/s batched
    - Time for 50K records: ~18-24 hours
    
  Tier 2 (classification): Qwen2.5-7B-Instruct
    - Interaction severity, drug class, food/timing
    - Requires: 1× RTX 4090 24GB (or any 24GB+ GPU)
    - Throughput: ~3000 tok/s
    - Time for 50K records: ~3-5 hours

QUERY-TIME (real-time, clinician is waiting):
  Composition: Qwen2.5-7B-Instruct
    - Formats SQL/graph results into cited natural-language answer
    - First-token latency: <200ms on RTX 4090
    - The prompt is simple: all hard work done in SQL/graph
    - 7B is sufficient because it's composing, not reasoning

ALTERNATIVE (single-model simplicity):
  Qwen2.5-32B-Instruct-AWQ on 1× A100 80GB
    - Handles both extraction and composition
    - Slightly lower quality on complex dosing table parsing vs 72B
    - Good enough for MVP, upgrade to 72B later if needed

EMBEDDING:
  BAAI/bge-large-en-v1.5 (335M params)
    - Runs on CPU (no GPU needed)
    - Serve via Hugging Face TEI (Text Embeddings Inference)
```

---

## 2. DATA SOURCES

### 2.1 Source Systems

| Source | Records | What It Provides | Update Cadence |
|--------|---------|-------------------|----------------|
| openFDA (SPL) | ~50K labels | Full FDA label text, structured tables (dosing, PK, interactions, adverse events), NDC/UNII identifiers | Monthly full, daily delta |
| DailyMed | ~50K labels | Same label with hierarchical subsection structure (e.g., 2.1 Adults, 2.2 Pediatric) | Monthly |
| RxNorm | ~4 RXCUIs per label | Clinical/branded formulation strings, synonyms, dose forms. Bridge to e-prescribing | Monthly |
| DrugBank | ~3 entries per label | Per-ingredient pharmacology, structured drug-drug interactions, classification | Quarterly (licensed) |
| Indian Brand Data | Your JSON file | Brand name, manufacturer, generic name, strength, dosage form, pack size | Manual updates |

### 2.2 Raw Data Format

Each formulation arrives as a single JSON object with four top-level keys:
```json
{
  "rxnorm":   [ ... ],       // array of RxNorm formulation entries
  "openfda":  { ... },       // openFDA SPL label data
  "dailymed": { ... },       // DailyMed label data with subsections
  "drugbank": [ ... ]        // array of DrugBank entries (per ingredient)
}
```

### 2.3 Indian Brand Data Format

Expected JSON structure (one object per brand-strength-form combination):
```json
[
  {
    "brand_name": "Nelficine",
    "manufacturer": "Cipla",
    "generic_name": "Nelfinavir Mesylate",
    "strength": "250 mg",
    "dosage_form": "Film Coated Tablet",
    "route": "Oral",
    "pack_size": "10s",
    "schedule": "H",
    "mrp": 450.00,
    "is_combination": false,
    "combination_ingredients": []
  },
  {
    "brand_name": "Tenolam-E",
    "manufacturer": "Cipla",
    "generic_name": "Tenofovir Disoproxil Fumarate + Emtricitabine + Efavirenz",
    "strength": "300mg + 200mg + 600mg",
    "dosage_form": "Tablet",
    "route": "Oral",
    "pack_size": "30s",
    "schedule": "H",
    "mrp": 1200.00,
    "is_combination": true,
    "combination_ingredients": [
      {"name": "Tenofovir Disoproxil Fumarate", "strength": "300 mg"},
      {"name": "Emtricitabine", "strength": "200 mg"},
      {"name": "Efavirenz", "strength": "600 mg"}
    ]
  }
]
```

---

## 3. EXECUTION SEQUENCE

The build is organized into 8 phases. Each phase has clear inputs, outputs,
and a definition of done. Phases are strictly sequential — each depends on
the output of the previous one.

```
PHASE 1: Database Setup
    ↓
PHASE 2: Pass 1 — Deterministic Transform (raw JSON → unified record)
    ↓
PHASE 3: Pass 2 — Structured Extraction (LLM-assisted)
    ↓
PHASE 4: Pass 3 — Chunking + Vector Embedding
    ↓
PHASE 5: Graph Database Population
    ↓
PHASE 6: Indian Brand Mapping
    ↓
PHASE 7: REST API Services (9 endpoints)
    ↓
PHASE 8: Integration Testing
```

### PHASE 1: Database Setup

**Goal**: Create all Postgres tables, pgvector extension, Neo4j schema, and Docker Compose infrastructure.

**Steps**:
```
1.1  Write docker-compose.yml with services:
       - postgres:16 (with pgvector extension)
       - neo4j:5
       - vllm (embedding model: bge-large-en-v1.5)
       - vllm (extraction model: qwen2.5-72b or 32b)
       - vllm (composition model: qwen2.5-7b)
       - fastapi-app
       
1.2  Create Postgres schema:
       - drug (formulation-level)
       - drug_identifier (RXCUI, NDC, UNII, DrugBank ID)
       - active_ingredient
       - inactive_ingredient
       - drug_indication (with ICD-10, SNOMED)
       - drug_interaction (with severity, magnitude, mechanism)
       - contraindication
       - dosing_regimen (with full population filters)
       - adverse_event
       - warning
       - population_approval
       - administration_timing
       - available_strength
       - rxnorm_formulation
       - label_table (JSONB rows)
       - clinical_section (full narrative text)
       - indian_brand
       - indian_brand_ingredient (FDC junction table)
       - rag_chunk (text + embedding via pgvector)
       
1.3  Create pgvector extension and embedding column:
       ALTER TABLE rag_chunk ADD COLUMN embedding vector(1024);
       CREATE INDEX ON rag_chunk USING ivfflat (embedding vector_cosine_ops);
       
1.4  Create Neo4j constraints and indexes:
       CREATE CONSTRAINT FOR (d:Drug) REQUIRE d.formulation_id IS UNIQUE;
       CREATE CONSTRAINT FOR (i:Ingredient) REQUIRE i.name IS UNIQUE;
       CREATE CONSTRAINT FOR (e:Enzyme) REQUIRE e.name IS UNIQUE;
       CREATE INDEX FOR (d:Drug) ON (d.generic_name);
       
1.5  Run migrations, verify connectivity from FastAPI container.

INPUTS:  docker-compose.yml, SQL migration files, Neo4j Cypher setup script
OUTPUTS: Running database containers with empty schema
DONE WHEN: FastAPI can connect to Postgres, pgvector, and Neo4j; all tables exist
```

### PHASE 2: Pass 1 — Deterministic Transform

**Goal**: Transform each raw consolidated JSON into a unified record and load structured data into Postgres.

**Steps**:
```
2.1  For each raw JSON file in the input directory:
       a) Run transform_to_unified() → unified record dict
       b) INSERT into drug table (formulation_id, generic_name, brand_names, 
          manufacturer, product_type)
       c) INSERT into drug_identifier (one row per RXCUI, NDC, UNII, DrugBank ID)
       d) INSERT into active_ingredient (one row per active ingredient with UNII, 
          DrugBank ID, strength if extractable)
       e) INSERT into inactive_ingredient
       f) INSERT into rxnorm_formulation (one row per RxNorm clinical formulation)
       g) INSERT into contraindication (from openFDA contraindications_table)
       h) INSERT into drug_interaction (from DrugBank structured interactions, 
          with subject_substance_role = active_ingredient | excipient)
       i) INSERT into clinical_section (one row per narrative section with full text 
          and subsections as JSONB)
       j) INSERT into label_table (one row per table, rows stored as JSONB)
       k) Run extract_strengths() → INSERT into available_strength
       l) Run extract_administration_timing() → INSERT into administration_timing
       m) Run enrich_interactions_from_tables() → UPDATE drug_interaction severity 
          and magnitude where deterministic

2.2  Validate: count records per table, spot-check 10 random formulations.

INPUTS:  ~50K raw consolidated JSON files
OUTPUTS: Postgres tables populated with deterministic data
DONE WHEN: drug table has ~50K rows; drug_interaction has data; 
           available_strength populated for drugs with RxNorm entries
```

### PHASE 3: Pass 2 — Structured Extraction (LLM-Assisted)

**Goal**: Extract structured facts from narrative text using locally hosted LLMs.

**Steps**:
```
3.1  Start vLLM server with Qwen2.5-72B-Instruct-AWQ for Tier 1 extraction.
3.2  Start vLLM server with Qwen2.5-7B-Instruct for Tier 2 classification.

3.3  For each formulation_id in drug table:
       a) INDICATION EXTRACTION (Tier 1, ~1.1 KB prompt):
          - Read clinical_section WHERE section = 'indications_and_usage'
          - Send to 72B with indication extraction prompt
          - Parse JSON response → INSERT into drug_indication
          
       b) DRUG CLASS EXTRACTION (Tier 2, ~0.8 KB prompt):
          - Read mechanism_of_action + indications text
          - Send to 7B with drug class prompt
          - Parse JSON → UPDATE drug SET drug_class, atc_codes
          
       c) INTERACTION SEVERITY CLASSIFICATION (Tier 2, ~5.5 KB prompt per batch of 20):
          - SELECT interactions WHERE severity = 'unknown' LIMIT 20
          - Send batch to 7B with severity classification prompt
          - Parse JSON → UPDATE drug_interaction SET severity, mechanism
          - Repeat until no unknown-severity interactions remain
          
       d) DOSING REGIMEN EXTRACTION (Tier 1, ~8 KB prompt):
          - Read dosage_and_administration text + subsections + dosing tables
          - Send to 72B with dosing extraction prompt
          - Parse JSON → INSERT into dosing_regimen (with full population filters)
          
       e) POPULATION APPROVAL EXTRACTION (Tier 1, ~5.4 KB prompt):
          - Read pediatric_use, geriatric_use, use_in_pregnancy, 
            use_in_specific_populations texts
          - Send to 72B with population approval prompt
          - Parse JSON → INSERT into population_approval
          
       f) FOOD/TIMING LLM FALLBACK (Tier 2, only if regex extraction was incomplete):
          - Read dosage + drug_interactions text
          - Send to 7B with timing extraction prompt
          - Parse JSON → UPDATE administration_timing

3.4  Validation:
       - drug_indication should have at least 1 row per formulation
       - dosing_regimen should have at least 1 row per formulation
       - drug_interaction severity should have <5% 'unknown' remaining
       - population_approval should have 1 row per formulation

INPUTS:  Postgres tables from Phase 2, vLLM endpoints
OUTPUTS: Fully populated structured_facts tables
DONE WHEN: All validation checks pass
ESTIMATED TIME: 18-24 hours for 50K records on 72B; 3-5 hours on 7B
ESTIMATED COST: Electricity only (local hosting)
```

### PHASE 4: Pass 3 — Chunking + Vector Embedding

**Goal**: Generate retrieval chunks from unified records and embed them into pgvector.

**Steps**:
```
4.1  For each formulation_id:
       a) Generate narrative chunks from clinical_section table:
          - Split on subsection boundaries
          - If subsection > 2000 chars, split on sentence boundaries with 200-char overlap
          - Each chunk gets metadata: formulation_id, section, subsection_id, 
            generic_name, brand_names, rxcui[], semantic_type
            
       b) Generate fact chunks:
          - One chunk per interaction (semantic_type = 'fact.interaction')
          - One chunk per contraindication (semantic_type = 'fact.contraindication')
          - Include subject_substance, subject_substance_role in chunk text
          
       c) Generate table chunks:
          - Serialize each label_table to markdown
          - Tag with semantic_type = 'table.{dosing|interaction|adverse_event|...}'
          
       d) INSERT each chunk into rag_chunk table (chunk_id, formulation_id, 
          section, semantic_type, text, metadata JSONB)

4.2  Embed all chunks:
       a) Start TEI server with bge-large-en-v1.5
       b) Batch embed: SELECT chunk_id, text FROM rag_chunk WHERE embedding IS NULL
          LIMIT 1000 at a time
       c) UPDATE rag_chunk SET embedding = :vector WHERE chunk_id = :id

4.3  Build IVFFlat index:
       - After all embeddings are inserted, CREATE INDEX for fast ANN search
       - SET ivfflat.probes = 10 for query-time accuracy

INPUTS:  Postgres tables from Phases 2-3, TEI embedding server
OUTPUTS: rag_chunk table with ~350 chunks per formulation × 50K = ~17.5M chunks
DONE WHEN: All chunks have non-null embeddings; vector search returns relevant results
ESTIMATED TIME: ~4-6 hours for embedding 17.5M chunks
```

### PHASE 5: Graph Database Population

**Goal**: Build the Neo4j drug interaction and pharmacology graph.

**Steps**:
```
5.1  Create Drug nodes:
       FOR EACH row in drug table:
         CREATE (d:Drug {
           formulation_id, generic_name, brand_names, drug_class, 
           product_type, manufacturer
         })

5.2  Create Ingredient nodes:
       FOR EACH row in active_ingredient:
         MERGE (i:Ingredient {name, unii, drugbank_id, role: 'active'})
         MATCH (d:Drug {formulation_id})
         CREATE (d)-[:CONTAINS_ACTIVE {strength}]->(i)
       
       FOR EACH row in inactive_ingredient:
         MERGE (i:Ingredient {name, unii, drugbank_id, role: 'excipient'})
         MATCH (d:Drug {formulation_id})
         CREATE (d)-[:CONTAINS_EXCIPIENT]->(i)

5.3  Create Enzyme nodes (from pharmacology extraction):
       FOR EACH drug with pharmacology.metabolism data:
         FOR EACH enzyme_name in metabolism[]:
           MERGE (e:Enzyme {name: enzyme_name})
           MATCH (d:Drug {formulation_id})
           CREATE (d)-[:METABOLISED_BY]->(e)
       
       FOR EACH drug with pharmacology.targets data:
         FOR EACH target_name in targets[]:
           MERGE (t:Target {name: target_name})
           MATCH (d:Drug {formulation_id})
           CREATE (d)-[:TARGETS]->(t)

5.4  Create DrugClass nodes:
       FOR EACH drug with drug_class data:
         FOR EACH class_name in drug_class[]:
           MERGE (c:DrugClass {name: class_name})
           MATCH (d:Drug {formulation_id})
           CREATE (d)-[:BELONGS_TO_CLASS]->(c)

5.5  Create Indication nodes:
       FOR EACH row in drug_indication:
         MERGE (ind:Indication {icd10, term})
         MATCH (d:Drug {formulation_id})
         CREATE (d)-[:INDICATED_FOR {population, line_of_therapy}]->(ind)

5.6  Create INTERACTS_WITH edges:
       FOR EACH row in drug_interaction:
         MATCH (a:Drug {formulation_id: subject_formulation_id})
         MATCH (b:Drug) WHERE b.generic_name CONTAINS partner_name 
           OR EXISTS((b)-[:CONTAINS_ACTIVE]->(:Ingredient {drugbank_id: partner_drugbank_id}))
         CREATE (a)-[:INTERACTS_WITH {
           severity, effect_direction, magnitude, mechanism,
           clinical_management, source, subject_substance_role
         }]->(b)

5.7  Create ALTERNATIVE_TO edges (derived):
       MATCH (a:Drug)-[:BELONGS_TO_CLASS]->(c:DrugClass)<-[:BELONGS_TO_CLASS]-(b:Drug)
       WHERE a <> b
       AND EXISTS((a)-[:INDICATED_FOR]->(:Indication)<-[:INDICATED_FOR]-(b))
       CREATE (a)-[:ALTERNATIVE_TO {shared_class: c.name}]->(b)

INPUTS:  Postgres tables from Phases 2-3
OUTPUTS: Neo4j graph with Drug, Ingredient, Enzyme, Target, DrugClass, 
         Indication nodes and relationship edges
DONE WHEN: MATCH (n) RETURN count(n) shows expected node counts;
           drug interaction edges match Postgres count
```

### PHASE 6: Indian Brand Mapping

**Goal**: Load Indian brand JSON data and map to FDA formulation records.

**Steps**:
```
6.1  Parse Indian brand JSON file:
       FOR EACH entry:
         a) Normalize generic_name → strip salt suffixes → normalized_generic_name
         b) Normalize dosage_form → form_canonical
         c) Parse strength → strength_value + strength_unit
         d) Detect FDC: if generic_name contains '+' or is_combination == true
         e) INSERT into indian_brand table

6.2  For FDC entries, decompose ingredients:
       FOR EACH FDC brand:
         FOR EACH ingredient in combination_ingredients[]:
           a) Normalize ingredient name
           b) INSERT into indian_brand_ingredient (brand_id, ingredient_index, 
              ingredient_name, ingredient_strength)

6.3  Run mapping (link indian_brand.formulation_id to drug.formulation_id):
       Pass 1 — Exact match:
         JOIN ON normalized_generic_name = normalized(drug.generic_name)
           AND strength_value = available_strength.strength_value
           AND form_canonical matches product.dosage_forms
         SET match_confidence = 'exact'
       
       Pass 2 — Normalized match (any strength):
         JOIN ON normalized_generic_name AND route
         SET match_confidence = 'normalized'
       
       Pass 3 — Fuzzy match (trigram similarity > 0.7):
         JOIN ON similarity(normalized_generic_name, normalized_fda_name) > 0.7
         SET match_confidence = 'fuzzy'

6.4  For FDC ingredients, map each to its own formulation_id:
       FOR EACH row in indian_brand_ingredient:
         Match ingredient_name to drug.generic_name (normalized)
         UPDATE formulation_id

6.5  Report unmatched brands for manual review:
       SELECT brand_name, generic_name_raw FROM indian_brand 
       WHERE formulation_id IS NULL

6.6  Add Indian brand nodes to Neo4j:
       FOR EACH mapped indian_brand:
         CREATE (ib:IndianBrand {brand_name, manufacturer_india, 
                strength_label, form_canonical, mrp_inr, schedule})
         MATCH (d:Drug {formulation_id})
         CREATE (ib)-[:BRAND_OF]->(d)

INPUTS:  Indian brand JSON file, Postgres drug table from Phase 2
OUTPUTS: indian_brand table fully mapped; Neo4j IndianBrand nodes linked
DONE WHEN: >90% of Indian brands have a formulation_id match;
           unmatched list reviewed manually
```

### PHASE 7: REST API Services

**Goal**: Build 9 FastAPI endpoints, one per clinical question.

**Steps**:
```
7.1  Build shared infrastructure:
       a) Database connection pool (asyncpg for Postgres)
       b) Neo4j async driver
       c) vLLM client (OpenAI-compatible endpoint)
       d) Embedding client (TEI endpoint)
       e) Entity resolver: drug name → formulation_id(s)
          (handles Indian brand names, generic names, FDC decomposition)
       f) Response composer: takes SQL/graph results + RAG chunks → LLM prompt → 
          cited response
       g) Indian brand translator: formulation_id → available Indian brands

7.2  Implement each endpoint (detailed in Section 6 of this PRD)

7.3  Add middleware:
       a) Request validation (Pydantic models)
       b) Response schema enforcement
       c) Audit logging (every query + response logged)
       d) Rate limiting
       e) Error handling with clinical-safe fallbacks

INPUTS:  All Postgres tables, Neo4j graph, vLLM endpoints
OUTPUTS: 9 REST endpoints serving clinical queries
DONE WHEN: All 9 endpoints return valid responses for test cases
```

### PHASE 8: Integration Testing

**Goal**: Validate all 9 queries end-to-end against known clinical scenarios.

**Steps**:
```
8.1  Run unit tests for each component:
       - Salt normalizer, dosage form normalizer
       - Entity resolver (brand → formulation_id)
       - Each SQL query template
       - Each Neo4j Cypher query
       - Chunk retrieval quality
       - LLM composition output format

8.2  Run integration tests (Section 9 of this PRD):
       - 10+ test cases per query template
       - Known drug pairs with expected interaction severity
       - Known dosing scenarios with expected computed doses
       - FDC decomposition scenarios
       - Edge cases: unmatched brands, missing data, ambiguous names

8.3  Clinical validation:
       - 50 clinical vignettes reviewed by a practitioner
       - Every response checked for: correct drug, correct dose, 
         no missed interactions, correct citations

INPUTS:  Running system from Phase 7, test case suite
OUTPUTS: Test report with pass/fail per test case
DONE WHEN: 100% of critical tests pass; <5% non-critical failures
```