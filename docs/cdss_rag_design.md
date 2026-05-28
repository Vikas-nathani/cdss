# CDSS RAG Architecture — Design Document

**Source data**: openFDA (SPL) + DailyMed + RxNorm + DrugBank, ~50K formulations
**Goal**: Clinical Decision Support System that answers
  1. Which medications treat disorder X?
  2. Do drugs A, B, C the patient is on interact?
  3. What are alternatives to drug X?
  4. What dose of drug X for this patient (age, sex, weight, comorbidities, current meds)?

---

## 1. Data model recap

One unified record per SPL set. Every record has five kinds of content, each going to a different store:

| Content type | Example | Best store | Why |
|---|---|---|---|
| Identifiers (RXCUI, NDC, UNII, DB id) | `212118`, `63010-010`, `98D603VP8V` | Relational (Postgres) | Exact-match joins on prescriptions, EHR orders |
| Structured facts (interactions, contraindications, dosing regimens) | "Nelfinavir + Simvastatin → ↑AUC 505%" | Relational + Graph | Filterable lookup; graph for transitive "A interacts with B interacts with C" |
| Narrative sections | "2.1 Adults and Adolescents..." | Vector (pgvector/Chroma) | Semantic similarity over free text |
| Label tables | Dosing tables, PK tables | Vector (serialised as markdown) + Relational | LLM sees them verbatim; also queryable |
| Provenance / citations | `source_span` on every chunk | Travels with chunks | Required for clinician trust |

---

## 2. Storage architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       INGEST PIPELINE                            │
│                                                                  │
│   raw consolidated JSON (per formulation)                        │
│              │                                                   │
│              ▼                                                   │
│   transform_to_unified.py  →  unified_record.json                │
│              │                                                   │
│              ▼                                                   │
│   NLP pass 2 (LLM + rules):                                      │
│     • indication → ICD-10 / SNOMED                               │
│     • interaction severity classification                        │
│     • dosing regimen population extraction (age, weight, renal)  │
│     • adverse event → MedDRA PT                                  │
│     • drug class inference (if not in openFDA pharm_class_*)     │
│              │                                                   │
│              ▼                                                   │
│   chunk_for_rag.py  →  chunks.jsonl                              │
│                                                                  │
└───────────────┬──────────────────────────────────────────────────┘
                │
     ┌──────────┼──────────┬──────────────────┐
     ▼          ▼          ▼                  ▼
┌─────────┐ ┌────────┐ ┌────────────┐ ┌──────────────────┐
│Postgres │ │Vector  │ │Graph (Neo4j│ │Object store (S3) │
│(facts + │ │(pgvec/ │ │ or property│ │ full labels +    │
│ lookups)│ │Chroma) │ │ graph)     │ │ archived PDFs    │
└─────────┘ └────────┘ └────────────┘ └──────────────────┘
```

### 2.1 Postgres (source of truth)

Normalised relational tables for the facts that CDSS actually filters on. Five core tables drive 80% of queries:

- `drug` — one row per formulation (formulation_id PK, generic_name, brand_names, manufacturer, product_type)
- `drug_identifier` — (formulation_id, id_type, id_value) where id_type ∈ {rxcui, ndc, unii, drugbank, application_number}
- `drug_interaction` — (subject_formulation_id, partner_name, partner_rxcui, partner_drugbank_id, severity, effect_direction, mechanism, management_text, source, source_document_id)
- `drug_indication` — (formulation_id, indication_text, icd10, snomed, population, line_of_therapy)
- `dosing_regimen` — (formulation_id, indication, age_min, age_max, weight_min, weight_max, renal, hepatic, pregnancy, dose_value, dose_unit, dose_basis, frequency, max_daily_dose, notes)

Secondary tables: `adverse_event`, `contraindication`, `active_ingredient`, `inactive_ingredient`, `rxnorm_formulation`, `label_table` (rows stored as JSONB).

Indexes required:
- `drug_interaction (subject_formulation_id)` and `(partner_rxcui)` and `(partner_drugbank_id)`
- `drug_indication (icd10)` and `(snomed)` and GIN on `indication_text` for fallback text search
- `dosing_regimen (formulation_id, indication)` with partial indexes for common population filters
- `drug_identifier (id_type, id_value)` — the universal lookup path

### 2.2 Vector store

Each chunk from `chunk_for_rag.py` is embedded and stored with its full metadata payload for hybrid filtering. Recommended: **pgvector in the same Postgres instance** — it lets the CDSS do SQL filters *and* vector search in one query, which matters because nearly every CDSS query has a hard filter.

Collection layout: one collection per `semantic_type` is tempting but a single collection with a `semantic_type` metadata filter is simpler and performs well up to ~10M chunks.

Embedding model choice:
- **Biomedical-tuned**: `pritamdeka/S-PubMedBERT-MS-MARCO` (good for symptoms/drug names) or `NeuML/pubmedbert-base-embeddings`
- **General, stronger**: `BAAI/bge-large-en-v1.5` — in practice gives competitive results on drug-label retrieval and is cheaper to run
- **Hosted**: Voyage `voyage-3` or OpenAI `text-embedding-3-large` if you want to skip self-hosting

Run an eval set (20–30 real clinician questions with gold-labelled correct chunks) against 2–3 models before committing.

### 2.3 Graph store (optional but high-leverage)

A property graph captures the reality that drugs, ingredients, classes, and diseases are a network. Node types: `Drug`, `Ingredient`, `DrugClass`, `Target` (enzyme/receptor), `Indication`, `Enzyme` (CYP3A4, etc.). Edge types: `INTERACTS_WITH` (severity, mechanism, source), `METABOLISED_BY`, `INHIBITS`, `INDUCES`, `INDICATED_FOR`, `CONTRAINDICATED_WITH`, `ALTERNATIVE_TO` (same class), `CONTAINS_ACTIVE`, `CONTAINS_EXCIPIENT`.

The graph answers queries that are awkward in SQL:
- *"My patient is on A and B. Any third drug I'm about to prescribe that shares a metabolic pathway?"* → 2-hop through `METABOLISED_BY`
- *"Alternatives to drug X that don't interact with anything my patient is on"* → filter by class, exclude nodes with `INTERACTS_WITH` edges to patient med set
- *"Why would these two drugs interact?"* → shortest path between them, returning enzyme/transporter intermediates

Neo4j, Memgraph, or Apache AGE (Postgres extension — keeps everything in one DB) all work. AGE has the lowest operational cost.

### 2.4 Object store

Full raw source JSONs, plus archived SPL PDFs and package inserts. Referenced by `source_document_id` in citations. S3/GCS/MinIO.

---

## 3. Retrieval patterns (one per CDSS use case)

Every pattern follows the same recipe: **hard filter first, vector search second, LLM last**. This is the single most important rule — never semantic-search a 50K-formulation corpus when you can pre-filter to 10 candidates first.

### 3.1 Disorder → candidate medications

```
Input: "first-line medication for uncomplicated UTI in adult women"
```

1. **Extract clinical entities** (small LLM call or scispaCy):
   - condition: "uncomplicated urinary tract infection"
   - population: adult, female
   - line_of_therapy: first-line

2. **Map to codes** (lookup in an internal ICD-10/SNOMED map; if unknown, fall back to semantic search over `drug_indication.indication_text`):
   - ICD-10: N39.0
   - SNOMED: 68566005

3. **SQL filter**:
   ```sql
   SELECT d.formulation_id, d.generic_name, d.brand_names
   FROM drug d
   JOIN drug_indication i USING (formulation_id)
   WHERE (i.icd10 = 'N39.0' OR i.snomed = '68566005')
     AND (i.population IS NULL OR i.population ILIKE '%adult%')
     AND (i.line_of_therapy IN ('first-line','unspecified') OR i.line_of_therapy IS NULL)
   ```

4. **Rank** the candidates — by evidence level, then by market presence (NDC count as a rough proxy for availability), then by whether a structured dosing regimen exists for the target population. Always return multiple candidates and let the clinician choose.

5. **For each top-N candidate, retrieve supporting chunks** from the vector store filtered by `formulation_id IN (candidates) AND semantic_type IN ('indications_and_usage','clinical_studies','dosage_and_administration')`. This gives the LLM the evidence to explain *why* each is appropriate.

6. **LLM synthesis**: compose the answer with inline citations to `source_span`. The LLM's job is to explain trade-offs and format the answer — never to invent the candidate list.

**Fallback when no ICD-10 match**: semantic search over the `drug_indication.indication_text` column (pg_trgm or a small embedding on indication text alone), then confirm matches by running the mapped candidates back through the LLM with "is this drug actually indicated for {condition}?"

### 3.2 Drug-drug interaction check

This is the highest-stakes query. It must never false-negative.

```
Input: patient on [Nelfinavir, Simvastatin, Warfarin]; adding [Rifampin]
```

1. **Resolve each med to a formulation_id** via RXCUI (from the prescription order) or exact-match on generic_name → `drug_identifier`.

2. **Pairwise SQL lookup** over all C(n,2) pairs (6 pairs for n=4):
   ```sql
   SELECT severity, effect_direction, mechanism, management_text, source_document_id
   FROM drug_interaction
   WHERE (subject_formulation_id = :a AND (partner_rxcui = :b_rxcui OR partner_drugbank_id = :b_db))
      OR (subject_formulation_id = :b AND (partner_rxcui = :a_rxcui OR partner_drugbank_id = :a_db))
   ```
   This is a direct hit — zero LLM involvement in the *detection* step.

3. **Class-level interactions**: also check whether any med belongs to a class listed in another med's `contraindication` table:
   ```sql
   SELECT c.drug_class, c.reason FROM contraindication c
   WHERE c.formulation_id = :a AND c.drug_class IN (SELECT class FROM drug_class WHERE formulation_id = :b)
   ```

4. **Graph traversal for shared-pathway risks** (optional, high-value):
   ```
   MATCH (a:Drug)-[:METABOLISED_BY]->(e:Enzyme)<-[:INHIBITS|INDUCES]-(b:Drug)
   WHERE a.id IN $patient_meds AND b.id IN $patient_meds
   RETURN a, b, e
   ```
   Catches interactions that aren't in any single drug's label but emerge from the pharmacology.

5. **LLM presentation**: for each hit, fetch the verbatim `management_text` and `source_span` excerpt from the vector store and have the LLM format the clinical recommendation. The LLM may not downgrade severity.

**Critical rule**: the interaction table is the authority. If the table says `severity=contraindicated`, the UI shows a hard block, period. The LLM narrates but does not decide.

### 3.3 Finding alternatives

```
Input: "alternative to Nelfinavir for a patient also on Rifampin"
         (Rifampin is contraindicated with Nelfinavir)
```

1. **Determine the therapeutic class** of the target drug from `drug.drug_class` or DrugBank classification.

2. **SQL candidate list**: all drugs in the same class, minus the target:
   ```sql
   SELECT formulation_id, generic_name FROM drug
   WHERE :target_class = ANY(drug_class) AND formulation_id != :target_id
   ```

3. **Filter by interaction compatibility** with the patient's other meds: for each candidate, run the interaction check (§3.2) against the patient's med list. Exclude anything with `severity IN ('contraindicated','major')`.

4. **Filter by indication overlap**: candidate must share at least one indication with the original drug (`drug_indication.icd10` join).

5. **Filter by patient population**: if the patient is pediatric/geriatric/pregnant, only keep candidates with a matching `dosing_regimen`.

6. **Rank** remaining candidates: fewer total interactions with patient's med list → higher score; same route as original → bonus; broader indication overlap → bonus.

7. **LLM**: present top 3–5 with the specific rationale for each ranking decision and the dosing regimen for this patient.

### 3.4 Dose recommendation

```
Input: drug=Nelfinavir, age=9, weight=25kg, sex=F, renal=normal, hepatic=mild
       current_meds=[Rifabutin]
```

1. **Look up regimens** for this drug and patient population:
   ```sql
   SELECT * FROM dosing_regimen
   WHERE formulation_id = :drug_id
     AND (age_min IS NULL OR :age >= age_min)
     AND (age_max IS NULL OR :age <= age_max)
     AND (weight_min IS NULL OR :weight >= weight_min)
     AND (weight_max IS NULL OR :weight <= weight_max)
     AND (pregnancy = :pregnancy OR pregnancy = 'any')
     AND (renal IN (:renal, 'any'))
     AND (hepatic IN (:hepatic, 'any'))
   ORDER BY specificity_score DESC
   ```
   `specificity_score` is the number of non-null/non-'any' criteria on the row — more specific rows win.

2. **Apply adjustments** from `adjustment_required_for`: if the patient's current meds include anything in any regimen's adjustment list, surface the adjustment note (e.g., "1250 mg BID is preferred dose when coadministered with rifabutin" — a real adjustment from this sample's label).

3. **Compute concrete dose** if the regimen is weight-based: `dose_value × weight` for `dose_basis='per_kg'`, then round to nearest manufactured strength from `active_ingredients.strength_value`.

4. **Retrieve the dosing narrative chunks** (`semantic_type='dosage_and_administration'`, filtered to subsections tagged with the matching age group) plus any dosing tables.

5. **LLM formats the recommendation** with: the computed dose, the matching regimen row, the verbatim table citation, and any adjustment warnings. Always state the source section so the clinician can verify.

**Never let the LLM compute the dose unilaterally.** The computation happens in code. The LLM composes the explanation.

---

## 4. Ingest pipeline

### 4.1 Pass 1 — deterministic (transform_to_unified.py)

Runs in milliseconds per record. Handles:
- Merging openFDA narrative with DailyMed subsections
- RxNorm normalisation into generic/brand clinical formulations
- DrugBank interactions with `active_ingredient`/`excipient` role tagging
- Label table deduplication and semantic_type tagging
- Identifier union

### 4.2 Pass 2 — structured extraction (requires LLM + rules)

This is the pass that converts narrative into queryable facts. Budget: ~1–2 LLM calls per record × 50K records ≈ 50K–100K calls. Use a cheap model (Haiku, Gemini Flash, Llama 3.1 8B) with strict JSON output schemas.

**Indication extraction**:
- Input: `clinical.indications_and_usage.text`
- Output: array of `{term, icd10, snomed, population, line_of_therapy, source_span}`
- Prompt: "Extract each distinct indication. For each, return the condition term as written, the best-match ICD-10 code, population restrictions, and line of therapy if stated. If uncertain about a code, return null."

**Interaction severity**:
- Input: verbatim label text of the interaction, plus DrugBank description
- Output: `severity ∈ {contraindicated, major, moderate, minor, unknown}`
- Rule-based first: if the text contains "contraindicated", "must not be coadministered", "avoid" → contraindicated/major. Use LLM only for ambiguous cases.

**Dosing regimen extraction**:
- Input: the dosing subsection text *plus* any matching table (tables often have the precise numbers)
- Output: array of `dosing_regimen` rows with populated `population` filters
- This one benefits from few-shot prompting — include 3 examples from different drug classes.

**Adverse event MedDRA coding**:
- Input: `adverse_reactions.text` and `adverse_reactions_tables`
- Output: array of `{term, meddra_pt, frequency, incidence_pct, source_span}`
- MedDRA is licensed; for open-source pipelines substitute with the OHDSI Vocabulary or keep terms ungraded.

**Drug class inference**:
- First try openFDA `pharm_class_cs/epc/moa/pe` (not present in your sample, but usually in the full openFDA response)
- Fall back to LLM given mechanism + indication

### 4.3 Pass 3 — chunking (chunk_for_rag.py)

Deterministic. Run after pass 2 so the chunks inherit the enriched metadata.

### 4.4 Update cadence

- **openFDA / DailyMed**: monthly full refresh, daily delta via the openFDA API effective_date
- **RxNorm**: monthly (matches RxNorm's release schedule)
- **DrugBank**: on licence refresh (typically quarterly)

Track `last_ingested_at` and `provenance.*.effective_date` per record. When SPL effective_date changes, reprocess the record end-to-end.

---

## 5. Query-time pipeline

Every CDSS query goes through a consistent flow. The principle: deterministic routing, structured retrieval, LLM only for composition.

```
clinician question
        │
        ▼
┌─────────────────────────────────────────┐
│ Intent classifier (small LLM or rules)  │
│   → {indication_lookup,                 │
│      interaction_check,                 │
│      alternative_search,                │
│      dose_recommendation,               │
│      general_info}                      │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│ Entity extraction                       │
│   drugs, conditions, patient context    │
│   → resolve to formulation_id / ICD-10  │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│ Retrieval (SQL + graph + vector)        │
│   • SQL: exact hits on structured facts │
│   • Graph: pathway / alternative queries│
│   • Vector: narrative evidence for      │
│     whatever SQL returned               │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│ LLM composition                         │
│   strict prompt: cite every claim to    │
│   a source_span; no uncited statements  │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│ Post-check                              │
│   • every claim cites a chunk           │
│   • severity downgrades are rejected    │
│   • dose values match the SQL lookup    │
└─────────────────────────────────────────┘
        │
        ▼
    response + citations
```

### 5.1 Entity resolution

Drug names in questions come in many forms: brand ("Viracept"), generic ("nelfinavir"), generic with salt ("nelfinavir mesylate"), misspellings, abbreviations. Build a resolver that tries in order:
1. Exact match on `drug.generic_name` or `drug.brand_names`
2. Exact match on any `rxnorm_formulation.synonym`
3. Trigram similarity (pg_trgm) with threshold 0.6 over generic + brand + synonyms, return top 3
4. If still ambiguous, ask the clinician

### 5.2 Prompt template for composition (dose recommendation example)

```
You are a clinical information assistant. Use ONLY the retrieved evidence below.
Every factual claim must cite a source like [src: <source_span.section_code>].
Do not compute doses — the computed dose is given in the patient_context block.
If evidence is missing for any part of the answer, say so explicitly.

Patient: {age}y {sex}, {weight}kg, renal={renal}, hepatic={hepatic}, pregnancy={pregnancy}
Current medications: {current_meds}

Drug requested: {drug_generic} ({drug_brand})

Computed dose (from structured lookup): {computed_dose}
Matched regimen: {regimen_row}
Required adjustments: {adjustments}

Retrieved evidence:
<chunk_1> section={section_1} source_span={span_1} text={text_1} </chunk_1>
<chunk_2> ... </chunk_2>
...

Answer in this structure:
1. Recommended dose (cite the matched regimen and dosing table)
2. Adjustments for this patient (cite each)
3. Administration notes (food, timing, interactions to monitor)
4. What to counsel the patient on
```

### 5.3 Guardrails

- **Hard blocks** on contraindicated combinations — UI refuses to proceed, no LLM override.
- **Severity floor** — if any retrieval tool returns `severity=contraindicated`, the composition LLM cannot produce a recommendation that proceeds; it must explain the block.
- **Dose sanity check** — if the LLM-composed answer contains a dose that differs from the SQL-computed dose, reject and regenerate.
- **Citation requirement** — post-generation check that every sentence with a factual claim has at least one source_span reference. This is testable with a simple regex over the response.
- **Escalation** — every response surfaces "this is decision support, not a prescription" and a one-click "view full label" that opens the raw SPL.

---

## 6. Evaluation

Before shipping, build three eval sets:

1. **Retrieval eval** (~200 questions × gold chunks)
   - "Show me the pediatric dosing section for nelfinavir" → gold: `formulation_id X, section dosage_and_administration, subsection 2.2`
   - Metric: recall@5 on the chunk set for each question.

2. **Interaction eval** (~500 known drug pairs with ground-truth severity)
   - Pull from OHDSI DDI datasets, FDA AERS, or manually curated
   - Metric: precision + recall on severity detection. False negatives (missed contraindications) are unacceptable — target recall > 99% for contraindicated pairs.

3. **End-to-end clinical scenario eval** (~50 multi-step cases)
   - Scripted clinical vignettes with expected structured answers
   - Metric: % of vignettes where the CDSS returns the expected drug/dose/warning set
   - Review with clinicians; no automated judge

Run all three on every ingest refresh and every prompt change. Ship only when all three meet their thresholds.

---

## 7. What's deliberately out of scope

- **Prescribing** — the CDSS advises; it does not write orders. Integration with e-prescribing happens in a separate system with its own safety review.
- **Patient-specific labs** — vital for real dosing (creatinine clearance, LFTs) but lives in the EHR, not in drug label data. The CDSS should accept labs as input but does not store them.
- **Off-label use** — the structured data is on-label only. Off-label reasoning requires evidence synthesis from PubMed + guidelines and is a larger project.
- **Pricing / formulary** — different data source entirely (Medi-Span, First Databank).
- **Patient communication** — `information_for_patients` and `spl_patient_package_insert` are preserved in the record, but rendering patient-facing content has its own compliance bar and should be a separate product surface.

---

## 8. Open design questions for your team

1. **Single vs multi-tenant**: will the same deployment serve multiple hospitals with different formularies? If yes, add a tenant dimension and formulary filter at the SQL layer.
2. **Licensing**: DrugBank commercial licence required for production use of the structured interactions. MedDRA licence required for PT coding. Budget for both.
3. **Language**: openFDA labels are English. If you need Hindi/Telugu/etc., add a translation layer — translate chunks at retrieval time, not at ingest (cheaper, stays in sync with label updates).
4. **Model choice**: cheap + fast (Haiku/Flash) for composition is usually enough. Reserve a larger model (Claude Opus, GPT-4) for the pass-2 structured extraction where one-time quality matters more than per-call cost.
5. **Audit log**: every CDSS response should be logged with the retrieved chunks, the LLM prompt, and the final answer. Required for any regulated deployment and for post-hoc debugging.

---

## 9. Files in this design

| File | Purpose |
|---|---|
| `cdss_unified_schema.json` | JSON Schema (draft-07) for the unified record |
| `transform_to_unified.py` | Raw consolidated JSON → unified record (pass 1) |
| `chunk_for_rag.py` | Unified record → retrieval chunks (pass 3) |
| `unified_sample.json` | The Viracept sample transformed |
| `chunks_sample.jsonl` | 362 retrieval chunks from the sample |

Pass 2 (structured extraction) is the next implementation step and has the highest leverage on CDSS quality.