# CDSS DrugDB — Clinical Decision Support System

Production-grade drug database pipeline backed by FDA/DrugBank/RxNorm/DailyMed data. Provides structured drug data, interaction graphs, dosing regimens, indications, and semantic search for clinical decision support applications.

## System Overview

The system ingests 738,197 drug records from four sources:

| Source | Records |
|---|---|
| OpenFDA | 256,165 |
| DailyMed | 51,731 |
| DrugBank | 19,842 |
| RxNorm | 410,459 |

Data is normalized, loaded into PostgreSQL (`cdss` schema), mirrored into a Neo4j graph, enriched with LLM-extracted structure (severity, mechanism, dosing, indications), and made searchable via pgvector embeddings.

## Prerequisites

### Services

| Service | Address |
|---|---|
| PostgreSQL | `$DB_HOST:5432` (database: `cdss`) |
| Neo4j | `bolt://localhost:7687` |
| Python | 3.10+ |

### Environment Variables

Copy `config/.env.example` to `.env` at the project root and fill in real values:

```bash
cp .env.example .env
```

Required variables:
```
DATABASE_HOST=$DB_HOST
DATABASE_PORT=5432
DATABASE_NAME=cdss
DATABASE_USER=cdss_app
DATABASE_PASSWORD=<PASSWORD>
DATABASE_URL=postgresql+asyncpg://cdss_app:<PASSWORD>@$DB_HOST:5432/cdss

POSTGRES_PASSWORD=<PASSWORD>

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<PASSWORD>

HF_TOKEN=<VALUE>
RUNPOD_API_KEY=<VALUE>

# For dosing regimen extraction (Stage 06):
DEEPSEEK_API_KEY=<VALUE>

# For drug interaction enrichment (Stage 04):
OPENROUTER_API_KEY=<VALUE>

# For indication extraction (Stage 07 — set when RunPod is running):
VLLM_EXTRACTION_URL=
VLLM_EMBEDDING_URL=
```

### Python Dependencies

```bash
pip install -r requirements.txt
```

## How to Run the Full Pipeline from Scratch

```bash
cd /home/nathanivikas890_gmail_com/cdss

# Run all stages in sequence
bash pipeline/run_pipeline.sh all
```

This will run Stages 00 through 11 in order. Estimated total time: 8–24 hours depending on LLM API rate limits.

### Stage execution order and notes

| Stage | Name | Runtime | Notes |
|---|---|---|---|
| 00 | Database Setup | 30–60 min | MRREL load is slow |
| 01 | Drug Table | 60–110 min | Standardization is the bottleneck |
| 02 | Ingredient Nodes | 15–25 min | |
| 03 | Drug-Ingredient Mapping | 30–45 min | |
| 04 | Drug Interactions | 4–12 hrs | LLM enrichment; checkpoint-resumable |
| 05 | Drug Class | 2–6 hrs | LLM extraction; checkpoint-resumable |
| 06 | Dosing Regimen | 6–24 hrs | LLM extraction; depends on Stage 09 |
| 07 | Indications | 4–12 hrs | Requires RunPod GPU |
| 08 | Clinical Sections | ~57 min | Depends on Stage 09 schema |
| 09 | Label Table | ~15 min | Run before 06 and 08 |
| 10 | Vector Embeddings | 4–24 hrs | Depends on 08 and 09 |
| 11 | Indian Brands | 30–45 min | Depends on 01, 02, 03 |

> Note: For a clean run, the recommended order is: 00 -> 01 -> 09 -> 08 -> 02 -> 03 -> 04 -> 05 -> 06 -> 07 -> 10 -> 11

## How to Run a Single Stage

```bash
# Run only Stage 04 (Drug Interactions)
bash pipeline/run_pipeline.sh 04

# Or run directly
cd pipeline/04_drug_interactions
python3 run.py
```

Each stage directory contains a `README.md` with detailed instructions, dependencies, and verification queries.

## How to Run the FastAPI Application

```bash
cd ~/cdss && \
  DATABASE_HOST=$DB_HOST \
  DATABASE_PORT=5432 \
  DATABASE_NAME=cdss \
  DATABASE_USER=cdss_app \
  DATABASE_PASSWORD=<PASSWORD> \
  NEO4J_URI=bolt://localhost:7687 \
  NEO4J_USER=neo4j \
  NEO4J_PASSWORD=<PASSWORD> \
  python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Or with the `.env` file loaded:
```bash
set -a && source .env && set +a
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

API will be available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

## Folder Map

```
cdss/
├── app/                        # FastAPI application (production — do not modify structure)
│   ├── main.py
│   ├── routers/
│   └── models/
├── tests/                      # Test suite
├── pipeline/                   # Data pipeline stages (NEW)
│   ├── run_pipeline.sh         # Master orchestrator
│   ├── 00_setup/               # DB schema + Neo4j + MRREL
│   ├── 01_drug_table/          # Core drug registry
│   ├── 02_ingredient_nodes/    # Ingredient canonical registry
│   ├── 03_drug_ingredient_mapping/  # Drug-ingredient relationships
│   ├── 04_drug_interactions/   # DDI data + LLM severity enrichment
│   ├── 05_drug_class/          # Drug class classification
│   ├── 06_dosing_regimen/      # LLM dosing extraction
│   ├── 07_indications/         # Phase 3 indication extraction (GPU)
│   ├── 08_clinical_sections/   # Label clinical sections
│   ├── 09_label_table/         # Structured label tables
│   ├── 10_vector_embeddings/   # RAG chunking + pgvector embeddings
│   └── 11_indian_brands/       # Indian brand mapping
├── sql/                        # All SQL files (NEW)
│   ├── schemas/                # DDL: table create statements
│   ├── migrations/             # ALTER / patch migrations
│   └── verification/           # Data quality / verification queries
├── scripts/                    # Utility and normalization scripts
│   ├── archive/                # Deprecated / one-off scripts
│   ├── compare_records.py
│   ├── normalize_*.py
│   ├── extract_*_schema.py
│   └── ...
├── data/
│   ├── raw/                    # Source/reference data files
│   ├── mappings/               # Field mappings, dosage form maps
│   ├── checkpoints/            # LLM extraction resume checkpoints
│   ├── exports/                # Generated exports and reports
│   └── samples/                # Sample data files for testing
├── logs/
│   ├── archive/                # Historical run logs
│   ├── 01_drug_table/
│   ├── 03_drug_ingredient_mapping/
│   ├── 05_drug_class/
│   ├── 06_dosing_regimen/
│   ├── 07_indications/
│   ├── 08_clinical_sections/
│   ├── 09_label_table/
│   └── ...
├── docs/
│   ├── architecture/           # PRD, API design, RAG design, deep research report
│   ├── schema/                 # Database schema documentation
│   └── reports/                # Population reports per stage
├── config/
│   ├── .env.example            # Template env file (no real secrets)
│   └── setup_cdss.sh           # GCP/server setup script
├── schemas/                    # Original schema files (kept in place)
├── reports/                    # Original reports (kept in place)
├── .env                        # Real env file (gitignored — never commit)
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── run_all.sh                  # Legacy pipeline script (preserved)
├── PIPELINE.md                 # Legacy pipeline documentation
└── README.md                   # This file
```

## Key Database Facts

| Item | Value |
|---|---|
| Postgres host | $DB_HOST:5432 |
| Postgres database | `cdss` (app schema) / `postgres` (source data) |
| Drug records | ~88,983 formulations in cdss.drug |
| Drug interactions | ~2,910,556 in cdss.drug_interaction |
| Clinical sections | ~2,887,910 in cdss.clinical_section |
| Label tables | ~510,527 in cdss.label_table |
| Neo4j | bolt://localhost:7687 (Drug, Ingredient, DrugClass nodes) |

## Documentation

- Architecture: `docs/architecture/`
- Schema docs: `docs/schema/`
- Stage-level: `PIPELINE.md`
- Pipeline history: `PIPELINE.md`, `CODEBASE_PIPELINE_DOCUMENTATION.md`
