# CDSS PRD — Master Index

## Document Structure

This PRD is organized into 4 sections with supporting schemas and pseudocode.

### Core Documents

| # | File | Contents |
|---|------|----------|
| 1 | `01_PRD_OVERVIEW.md` | Product overview, tech stack, 8-phase execution sequence with step-by-step instructions |
| 2 | `02_REST_API_DESIGN.md` | All 9 REST endpoints with full pseudocode: input parsing, entity resolution, SQL queries, Neo4j Cypher, vector search, LLM composition, Indian brand mapping, post-checks |
| 3 | `03_PROMPT_TEMPLATES.md` | LLM prompt templates for query-time composition (Q1-Q8) and Pass 2 batch extraction |
| 4 | `04_TEST_CASES.md` | 75+ test cases across all 9 queries, entity resolver, salt normalizer, dosage form normalizer, and cross-cutting concerns |

### Schemas & Infrastructure

| File | Contents |
|------|----------|
| `schemas/postgres_schema.sql` | Complete DDL: 20 tables, indexes, pgvector setup, entity resolver function, audit log |
| `schemas/neo4j_schema.cypher` | Node types, relationship types, constraints, indexes, query patterns |
| `schemas/docker-compose.yml` | Full Docker Compose: Postgres+pgvector, Neo4j, TEI embeddings, vLLM (composition + extraction), FastAPI |
| `schemas/Dockerfile` | FastAPI container build |

### Pseudocode (Implementation-Ready)

| File | Contents |
|------|----------|
| `pseudocode/pydantic_schemas.py` | All Pydantic request/response models for 9 endpoints |
| `pseudocode/fastapi_main.py` | FastAPI app entry point with routing, middleware, lifespan management |

### Previously Delivered (from earlier sessions)

| File | Contents |
|------|----------|
| `transform_to_unified.py` | Pass 1: raw JSON → unified record (working code) |
| `chunk_for_rag.py` | Pass 3: unified record → RAG chunks (working code) |
| `pass2_extractors.py` | Pass 2: LLM prompt builders + deterministic extractors (working code) |
| `indian_brand_mapper.py` | Salt normalizer, dosage form normalizer, mapping SQL, FDC handler (working code) |
| `cdss_unified_schema.json` | JSON Schema for the unified record format |
| `cdss_query_templates.py` | Query template logic with SQL patterns |

## How to Use This PRD

**For a code-generation LLM:**

1. Feed `01_PRD_OVERVIEW.md` for context and execution sequence
2. Feed `schemas/postgres_schema.sql` to generate database migrations
3. Feed `schemas/neo4j_schema.cypher` to generate graph population scripts
4. Feed `pseudocode/pydantic_schemas.py` + `pseudocode/fastapi_main.py` for the API scaffold
5. Feed `02_REST_API_DESIGN.md` one endpoint at a time to generate each handler
6. Feed `03_PROMPT_TEMPLATES.md` for the LLM integration layer
7. Feed `04_TEST_CASES.md` to generate pytest test files
8. Feed `schemas/docker-compose.yml` + `schemas/Dockerfile` for deployment

**Build order (matches Phase sequence):**

```
Phase 1: docker-compose.yml → postgres_schema.sql → neo4j_schema.cypher
Phase 2: transform_to_unified.py (already working)
Phase 3: pass2_extractors.py (already working, needs vLLM endpoint)
Phase 4: chunk_for_rag.py (already working, needs TEI endpoint)
Phase 5: Neo4j population script (generate from neo4j_schema.cypher)
Phase 6: indian_brand_mapper.py (already working)
Phase 7: fastapi_main.py → each endpoint from 02_REST_API_DESIGN.md
Phase 8: test cases from 04_TEST_CASES.md
```