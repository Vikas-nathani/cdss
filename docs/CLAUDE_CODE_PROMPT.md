# CLAUDE CODE PROMPT — CDSS Implementation

Copy everything below this line and paste it into Claude Code as your initial prompt.
Then follow the phased instructions at the bottom.

---

## CONTEXT

I am building a Clinical Decision Support System (CDSS) for Indian medical practitioners. The complete architecture, database schemas, API design, and pseudocode have already been designed. I need you to implement the production code.

## PROJECT STRUCTURE

The project is already set up at the current directory with this structure:

```
cdss/
├── docker-compose.yml           # Docker Compose for Postgres+pgvector, Neo4j, TEI, vLLM, FastAPI
├── Dockerfile                   # FastAPI container
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables (edit passwords before use)
├── schemas/
│   ├── postgres_schema.sql      # Full Postgres DDL (20+ tables, indexes, entity resolver function)
│   ├── postgres_schema_patch.sql# Patch for 12 additional fields
│   └── neo4j_schema.cypher      # Neo4j constraints and indexes
├── scripts/
│   ├── transform_to_unified.py  # WORKING: raw JSON → unified record
│   ├── chunk_for_rag.py         # WORKING: unified record → RAG chunks
│   ├── pass2_extractors.py      # WORKING: deterministic extractors + LLM prompt builders
│   ├── indian_brand_mapper.py   # WORKING: salt normalizer, dosage form normalizer, mapping logic
│   ├── neo4j_populate.py        # PSEUDOCODE: populates Neo4j from Postgres
│   └── cdss_query_templates.py  # REFERENCE: SQL/logic for all 9 query templates
├── app/
│   ├── main_pseudocode.py       # PSEUDOCODE: FastAPI app entry point
│   ├── models/
│   │   └── schemas_pseudocode.py# PSEUDOCODE: all Pydantic request/response models
│   ├── endpoints/               # EMPTY: 9 placeholder files to implement
│   └── core/                    # EMPTY: 8 placeholder files to implement
├── data/
│   ├── raw/                     # PUT 50K JSON files here
│   └── samples/
│       ├── sample_raw_input.json          # One sample raw JSON (Nelfinavir/Viracept)
│       ├── unified_sample.json            # Same, after Pass 1 transform
│       ├── unified_sample_enriched.json   # Same, after deterministic extractors
│       └── chunks_sample.jsonl            # 362 RAG chunks from that sample
├── docs/
│   ├── 00_INDEX.md              # Master index — READ THIS FIRST
│   ├── 01_PRD_OVERVIEW.md       # Product requirements, tech stack, 8-phase build sequence
│   ├── 02_REST_API_DESIGN.md    # Full pseudocode for all 9 endpoints + shared infrastructure
│   ├── 03_PROMPT_TEMPLATES.md   # LLM prompts for composition + extraction
│   ├── 04_TEST_CASES.md         # 75+ test cases
│   ├── cdss_rag_design.md       # Architecture document
│   └── cdss_unified_schema.json # JSON Schema for unified record
└── tests/                       # EMPTY: test files to implement
```

## TECHNOLOGY STACK

- **API**: Python 3.11+ with FastAPI
- **Database**: PostgreSQL 16 + pgvector extension (same DB for structured facts AND vector search)
- **Graph**: Neo4j 5.x (drug interaction pathways, alternative drug finding)
- **LLM serving**: vLLM with Qwen2.5-7B-Instruct (query-time composition) and Qwen2.5-72B-Instruct-AWQ (batch extraction)
- **Embeddings**: BAAI/bge-large-en-v1.5 via Hugging Face TEI
- **Deployment**: Docker Compose on a single server

## THE 9 CLINICAL QUERIES THIS SYSTEM ANSWERS

| Endpoint | Question | Needs LLM? |
|----------|----------|-------------|
| Q1 POST /api/v1/query/disorder-to-medications | Which drugs treat disorder X? | Yes |
| Q2 POST /api/v1/query/interaction-check | Do these drugs interact? | Yes |
| Q3 POST /api/v1/query/alternatives | Alternatives to drug X safe with patient's meds? | Yes |
| Q4 POST /api/v1/query/dose-recommendation | What dose for this patient (age/weight/renal/hepatic)? | Yes |
| Q5 POST /api/v1/query/population-approval | Is drug X approved for pediatric/geriatric/pregnant? | Yes |
| Q6 POST /api/v1/query/safe-drugs-for-condition | Drugs for condition Y not contraindicated with current meds? | Yes |
| Q7 POST /api/v1/query/organ-impairment-dosing | Dose adjustment for renal/hepatic impairment? | Yes |
| Q8 POST /api/v1/query/administration-timing | Take with food? Time separation from other drugs? | Sometimes |
| Q9 POST /api/v1/query/pill-burden | Which tablet strength = fewest pills per day? | No |

## CORE ARCHITECTURE RULE

**Deterministic retrieval first, LLM composition last.** Every clinical fact (dose, interaction severity, contraindication) comes from SQL or graph queries. The LLM only composes the natural-language response with citations. The LLM NEVER computes doses, classifies severity, or overrides structured data. Post-checks enforce this.

## WHAT IS ALREADY WORKING (do NOT rewrite these)

1. `scripts/transform_to_unified.py` — transforms raw JSON to unified record. Tested, correct.
2. `scripts/chunk_for_rag.py` — generates RAG chunks from unified record. Tested, 362 chunks.
3. `scripts/pass2_extractors.py` — regex extractors (strength, food timing, interaction enrichment) work. LLM prompt builders return prompt dicts ready to send to vLLM.
4. `scripts/indian_brand_mapper.py` — salt normalizer (15/15 tests pass), dosage form normalizer (10/10 pass), mapping SQL.

## WHAT NEEDS TO BE BUILT (in this order)

### TASK 1: Postgres Loader Script
**File**: `scripts/load_to_postgres.py`
**Read**: `schemas/postgres_schema.sql` + `data/samples/unified_sample_enriched.json`
**Do**: Write an async Python script that takes a unified record dict and INSERTs into all Postgres tables:
- drug (1 row)
- drug_identifier (multiple rows: rxcui, ndc, unii, drugbank, application_number)
- active_ingredient (1 row per active ingredient)
- inactive_ingredient (1 row per inactive ingredient)
- rxnorm_formulation (1 row per RxNorm entry)
- clinical_section (1 row per narrative section, subsections stored as JSONB)
- label_table (1 row per structured table)
- contraindication (from structured_facts.contraindications)
- drug_interaction (from structured_facts.interactions)
- available_strength (from structured_facts.available_strengths)
- administration_timing (from structured_facts.administration_timing)
- dosing_regimen (from structured_facts.dosing_regimens — may be empty before Pass 2)
- product_sku (from product.skus with physical characteristics)

Use asyncpg. Include a batch mode that processes a directory of unified JSON files.
Test by loading `data/samples/unified_sample_enriched.json` and verifying row counts.

### TASK 2: Embedding Pipeline Script
**File**: `scripts/embed_chunks.py`
**Read**: `scripts/chunk_for_rag.py` output format
**Do**: Script that:
1. Reads chunks from `chunks_sample.jsonl` (or generates them from a unified record)
2. Calls the TEI embedding endpoint (http://localhost:8081/embed) in batches of 32
3. INSERTs into rag_chunk table with the embedding vector
4. After all inserts, creates the IVFFlat index

### TASK 3: Pass 2 LLM Runner
**File**: `scripts/run_pass2.py`
**Read**: `scripts/pass2_extractors.py` (the prompt builders) + `docs/03_PROMPT_TEMPLATES.md`
**Do**: Script that:
1. For each formulation_id in the drug table
2. Calls `build_all_llm_prompts()` from pass2_extractors.py
3. Sends each prompt to vLLM (http://localhost:8083/v1/chat/completions)
4. Parses the JSON response
5. UPDATEs the appropriate Postgres tables (drug_indication, dosing_regimen, population_approval, drug_interaction severity, drug.drug_class)
Include retry logic and error handling. Log failures.

### TASK 4: Indian Brand Loader
**File**: `scripts/load_indian_brands.py`
**Read**: `scripts/indian_brand_mapper.py` + `schemas/postgres_schema.sql` (indian_brand table)
**Do**: Script that:
1. Reads an Indian brand JSON file (format defined in docs/01_PRD_OVERVIEW.md section 2.3)
2. Normalizes each entry (salt stripping, dosage form normalization)
3. Detects FDCs and decomposes ingredients
4. INSERTs into indian_brand and indian_brand_ingredient tables
5. Runs 3-pass mapping (exact → normalized → fuzzy) to link formulation_id
6. Reports unmatched brands

### TASK 5: FastAPI Core Infrastructure
**Files**: `app/core/database.py`, `app/core/llm_client.py`, `app/core/embedding_client.py`, `app/core/entity_resolver.py`, `app/core/vector_search.py`, `app/core/response_composer.py`, `app/core/indian_brand_mapper.py`, `app/core/post_checks.py`
**Read**: `docs/02_REST_API_DESIGN.md` (the "SHARED INFRASTRUCTURE" section at the top)
**Do**: Implement each module. Key details:
- `database.py`: asyncpg pool + neo4j async driver, connection lifecycle
- `llm_client.py`: async httpx client calling vLLM OpenAI-compatible endpoint
- `entity_resolver.py`: calls the `resolve_drug()` Postgres function (already in SQL schema)
- `vector_search.py`: builds pgvector query with metadata filters + cosine distance
- `response_composer.py`: assembles LLM prompt from SQL/graph/vector results using templates from `docs/03_PROMPT_TEMPLATES.md`
- `post_checks.py`: severity floor, dose sanity, citation presence checks

### TASK 6: Pydantic Models
**File**: `app/models/schemas.py`
**Read**: `app/models/schemas_pseudocode.py`
**Do**: Convert the pseudocode into proper importable Pydantic v2 models. All the types and fields are defined — just make it syntactically correct Python.

### TASK 7: FastAPI Main + Router
**File**: `app/main.py`
**Read**: `app/main_pseudocode.py`
**Do**: Convert to working FastAPI app with lifespan management, middleware, and route registration.

### TASK 8: Endpoint Handlers (9 files)
**Files**: `app/endpoints/q1_disorder_to_meds.py` through `app/endpoints/q9_pill_burden.py`
**Read**: `docs/02_REST_API_DESIGN.md` — each endpoint has complete pseudocode with exact SQL queries, Neo4j Cypher, vector search parameters, and LLM prompt templates
**Do**: Implement one endpoint at a time. Start with Q9 (simplest — pure math, no LLM), then Q8 (simple SQL lookup), then Q2 (interaction check — touches all three stores).

Each endpoint handler should:
1. Accept the Pydantic request model
2. Call entity_resolver for any drug names
3. Run the SQL query (exact SQL is in the pseudocode)
4. Run Neo4j Cypher if needed (exact Cypher is in the pseudocode)
5. Run vector search if needed
6. Compute values if needed (dose, pill burden)
7. Call response_composer with the LLM prompt template
8. Call indian_brand_mapper for localization
9. Run post_checks
10. Log to query_audit_log table
11. Return the Pydantic response model

### TASK 9: Tests
**Files**: `tests/test_normalizers.py`, `tests/test_entity_resolver.py`, `tests/test_q1.py` through `tests/test_q9.py`
**Read**: `docs/04_TEST_CASES.md` — every test case is specified with input, expected output, and what it validates
**Do**: Convert to pytest test files. Use pytest-asyncio for async tests.

## HOW TO APPROACH THIS

Work through the tasks in order (1 → 9). Each task builds on the previous one. After each task, verify it works before moving to the next:

- After Task 1: `SELECT count(*) FROM drug` should return 1 (the sample)
- After Task 2: `SELECT count(*) FROM rag_chunk WHERE embedding IS NOT NULL` should return 362
- After Task 5+6+7: `GET /health` should return all services connected
- After Task 8 (Q9): `POST /api/v1/query/pill-burden {"drug":"NELFINAVIR MESYLATE","daily_dose_mg":2500,"frequency":"BID"}` should return 625 MG as the recommended strength with 4 pills/day

## IMPORTANT RULES

1. Do NOT rewrite `transform_to_unified.py`, `chunk_for_rag.py`, `pass2_extractors.py`, or `indian_brand_mapper.py` — they are tested and working. Import from them.
2. Read the referenced doc file BEFORE implementing each task.
3. Use asyncpg for Postgres (not SQLAlchemy). Use neo4j async driver. Use httpx for HTTP clients.
4. All LLM calls go to local vLLM endpoints (see .env for URLs). Never call external APIs.
5. The entity resolver must handle Indian brand names, FDC decomposition, salt-stripped matching, and fuzzy matching — the SQL function in postgres_schema.sql already does this.
6. Every endpoint must log to query_audit_log table.
7. Post-checks are NOT optional — they prevent the LLM from overriding clinical safety data.

Start with Task 1. Read `schemas/postgres_schema.sql` and `data/samples/unified_sample_enriched.json`, then build `scripts/load_to_postgres.py`.

---

## PHASED EXECUTION FOR CLAUDE CODE

Paste the prompt above first. Then give these follow-up prompts one at a time, after each task is verified:

```
Prompt 1: "Start with Task 1. Read schemas/postgres_schema.sql and data/samples/unified_sample_enriched.json. Build scripts/load_to_postgres.py."

Prompt 2: "Task 1 is done. Now build Task 2: scripts/embed_chunks.py. Read scripts/chunk_for_rag.py for the chunk format."

Prompt 3: "Task 2 is done. Now build Task 3: scripts/run_pass2.py. Read scripts/pass2_extractors.py and docs/03_PROMPT_TEMPLATES.md."

Prompt 4: "Task 3 is done. Now build Task 4: scripts/load_indian_brands.py. Read scripts/indian_brand_mapper.py for normalizers."

Prompt 5: "Task 4 is done. Now build Tasks 5+6+7 together: app/core/*.py, app/models/schemas.py, app/main.py. Read docs/02_REST_API_DESIGN.md for the shared infrastructure section."

Prompt 6: "Tasks 5-7 are done. Now build Q9 endpoint: app/endpoints/q9_pill_burden.py. This is pure computation, no LLM. Read the Q9 section in docs/02_REST_API_DESIGN.md."

Prompt 7: "Q9 works. Now build Q8: app/endpoints/q8_administration_timing.py. Simple SQL lookup. Read the Q8 section in docs/02_REST_API_DESIGN.md."

Prompt 8: "Q8 works. Now build Q2: app/endpoints/q2_interaction_check.py. This needs Postgres + Neo4j + pgvector + LLM. Read the Q2 section in docs/02_REST_API_DESIGN.md."

Prompt 9: "Q2 works. Now build the remaining 6 endpoints: Q1, Q3, Q4, Q5, Q6, Q7. Read each section in docs/02_REST_API_DESIGN.md."

Prompt 10: "All endpoints work. Now build Task 9: tests. Read docs/04_TEST_CASES.md and convert to pytest files."
```
