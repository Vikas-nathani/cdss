# ============================================================================
# CDSS REST API — FastAPI Service Design
# ============================================================================
# 9 endpoints, one per clinical question.
# Each endpoint follows the same pipeline:
#   1. Parse + validate input
#   2. Resolve drug names → formulation_id(s)  [Entity Resolver]
#   3. SQL retrieval (Postgres)                  [Structured Facts]
#   4. Graph retrieval (Neo4j)                   [Pathway Queries]
#   5. Vector retrieval (pgvector)               [Narrative Evidence]
#   6. Computation (if needed)                   [Dose Calc, Pill Burden]
#   7. LLM composition (vLLM)                    [Natural Language Response]
#   8. Indian brand mapping                      [Localization]
#   9. Post-checks                               [Safety Guardrails]
#  10. Audit log                                 [Compliance]
# ============================================================================

# ============================================================================
# SHARED INFRASTRUCTURE (implement first, used by all endpoints)
# ============================================================================

# --- app/core/database.py ---
"""
PSEUDOCODE: Database connections

class DatabasePool:
    postgres: asyncpg.Pool
    neo4j: neo4j.AsyncDriver
    
    async def init():
        postgres = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
        neo4j = neo4j.AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    async def close():
        await postgres.close()
        await neo4j.close()
"""

# --- app/core/llm_client.py ---
"""
PSEUDOCODE: Local LLM client (OpenAI-compatible vLLM endpoint)

class LLMClient:
    composition_url: str   # http://vllm-composition:8000/v1/chat/completions
    
    async def compose(system_prompt: str, user_prompt: str, 
                      temperature: float = 0.0, max_tokens: int = 2000) -> str:
        response = await httpx.post(composition_url, json={
            "model": "Qwen/Qwen2.5-7B-Instruct",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {"type": "text"}
        })
        return response.json()["choices"][0]["message"]["content"]
"""

# --- app/core/embedding_client.py ---
"""
PSEUDOCODE: Embedding client (TEI endpoint)

class EmbeddingClient:
    tei_url: str   # http://embedding:80/embed
    
    async def embed(text: str) -> list[float]:
        response = await httpx.post(tei_url, json={"inputs": text})
        return response.json()[0]  # 1024-dim vector
    
    async def embed_batch(texts: list[str]) -> list[list[float]]:
        response = await httpx.post(tei_url, json={"inputs": texts})
        return response.json()
"""

# --- app/core/entity_resolver.py ---
"""
PSEUDOCODE: Entity Resolver — drug name → formulation_id(s)

Every endpoint calls this first. Handles:
  - Indian brand names (exact + fuzzy)
  - FDA generic names
  - Salt-form normalization
  - FDC decomposition (returns multiple formulation_ids)

async def resolve_drug(db: Pool, drug_name: str) -> list[ResolvedDrug]:
    rows = await db.fetch("SELECT * FROM resolve_drug($1)", drug_name)
    
    IF no rows:
        RAISE DrugNotFoundError(drug_name, suggestions=fuzzy_suggestions)
    
    results = []
    FOR row IN rows:
        results.append(ResolvedDrug(
            formulation_id = row["formulation_id"],
            match_type = row["match_type"],         # indian_brand_exact, fdc_ingredient, etc.
            matched_name = row["matched_name"],
            is_fdc_component = (row["match_type"] == "indian_fdc_ingredient")
        ))
    
    RETURN results
"""

# --- app/core/indian_brand_mapper.py ---
"""
PSEUDOCODE: Indian Brand Mapper — formulation_id → Indian brands

Called at the END of every endpoint to translate FDA formulation into Indian brands.

async def get_indian_brands(db: Pool, formulation_id: str, 
                             strength_filter: float = None) -> list[IndianBrand]:
    sql = '''
        SELECT brand_name, manufacturer_india, strength_label, strength_value,
               form_canonical, pack_size, schedule, mrp_inr
        FROM indian_brand
        WHERE formulation_id = $1
    '''
    params = [formulation_id]
    
    IF strength_filter:
        sql += ' AND strength_value = $2'
        params.append(strength_filter)
    
    sql += ' ORDER BY brand_name, strength_value'
    
    RETURN await db.fetch(sql, *params)
"""

# --- app/core/vector_search.py ---
"""
PSEUDOCODE: Vector search with hybrid filtering

async def search_chunks(db: Pool, embedding_client: EmbeddingClient,
                        query_text: str,
                        formulation_ids: list[str] = None,
                        semantic_types: list[str] = None,
                        sections: list[str] = None,
                        top_k: int = 5) -> list[RagChunk]:
    
    query_vector = await embedding_client.embed(query_text)
    
    # Build SQL with metadata filters + vector distance
    sql = '''
        SELECT chunk_id, formulation_id, section, subsection_title,
               semantic_type, text, source,
               1 - (embedding <=> $1::vector) as similarity
        FROM rag_chunk
        WHERE 1=1
    '''
    params = [query_vector]
    param_idx = 2
    
    IF formulation_ids:
        sql += f' AND formulation_id = ANY(${param_idx})'
        params.append(formulation_ids)
        param_idx += 1
    
    IF semantic_types:
        sql += f' AND semantic_type = ANY(${param_idx})'
        params.append(semantic_types)
        param_idx += 1
    
    IF sections:
        sql += f' AND section = ANY(${param_idx})'
        params.append(sections)
        param_idx += 1
    
    sql += f' ORDER BY embedding <=> $1::vector LIMIT ${param_idx}'
    params.append(top_k)
    
    RETURN await db.fetch(sql, *params)
"""

# --- app/core/response_composer.py ---
"""
PSEUDOCODE: Response Composer — assembles LLM prompt from SQL/graph/vector results

async def compose_response(
    llm: LLMClient,
    query_template: str,      # Q1, Q2, ..., Q9
    patient_context: dict,
    sql_results: dict,
    graph_results: dict,
    rag_chunks: list[RagChunk],
    computed_values: dict      # dose, pill count, etc.
) -> ComposedResponse:
    
    system_prompt = SYSTEM_PROMPTS[query_template]
    
    # Build evidence block from RAG chunks
    evidence_block = ""
    FOR i, chunk IN enumerate(rag_chunks):
        evidence_block += f"<evidence_{i+1} section='{chunk.section}' "
        evidence_block += f"subsection='{chunk.subsection_title}'>\n"
        evidence_block += f"{chunk.text}\n</evidence_{i+1}>\n\n"
    
    # Build user prompt from template
    user_prompt = PROMPT_TEMPLATES[query_template].format(
        patient_context = json.dumps(patient_context),
        sql_results = json.dumps(sql_results),
        graph_results = json.dumps(graph_results),
        computed_values = json.dumps(computed_values),
        evidence = evidence_block
    )
    
    raw_response = await llm.compose(system_prompt, user_prompt)
    
    RETURN ComposedResponse(
        answer = raw_response,
        citations = extract_citations(raw_response),
        chunks_used = [c.chunk_id for c in rag_chunks]
    )
"""

# --- app/core/post_checks.py ---
"""
PSEUDOCODE: Post-checks — safety guardrails applied after LLM composition

def run_post_checks(query_template: str, sql_results: dict, 
                     composed_response: ComposedResponse,
                     computed_values: dict) -> PostCheckResult:
    
    errors = []
    warnings = []
    
    # CHECK 1: Severity floor — LLM may not downgrade severity
    IF query_template IN ("Q2", "Q6"):
        FOR interaction IN sql_results.get("interactions", []):
            IF interaction["severity"] == "contraindicated":
                IF "may proceed" in composed_response.answer.lower():
                    errors.append("LLM attempted to override contraindicated interaction")
                IF "contraindicated" not in composed_response.answer.lower():
                    errors.append("LLM failed to mention contraindicated interaction")
    
    # CHECK 2: Dose sanity — LLM-stated dose must match computed dose
    IF query_template IN ("Q4", "Q7"):
        computed_dose = computed_values.get("computed_dose")
        IF computed_dose:
            # Extract dose from LLM response via regex
            mentioned_doses = re.findall(r'(\d+(?:\.\d+)?)\s*mg', composed_response.answer)
            IF mentioned_doses AND str(computed_dose) not in mentioned_doses:
                errors.append(f"LLM stated dose {mentioned_doses} but computed dose is {computed_dose}")
    
    # CHECK 3: Citation presence — every factual sentence should cite evidence
    sentences = split_sentences(composed_response.answer)
    uncited_factual = [s for s in sentences 
                       if is_factual_claim(s) and not has_citation(s)]
    IF len(uncited_factual) > len(sentences) * 0.3:
        warnings.append(f"{len(uncited_factual)} factual sentences lack citations")
    
    # CHECK 4: Contraindicated dose — if regimen says CONTRAINDICATED, block
    IF query_template IN ("Q4", "Q7"):
        IF computed_values.get("dose_amount") == "CONTRAINDICATED":
            errors.append("Drug is contraindicated for this population — block response")
    
    RETURN PostCheckResult(
        passed = len(errors) == 0,
        errors = errors,
        warnings = warnings
    )
"""

# ============================================================================
# ENDPOINT 1: POST /api/v1/query/disorder-to-medications  (Q1)
# ============================================================================
"""
REQUEST:
{
    "disorder": "HIV infection",           // free text or ICD-10 code
    "population": "adult",                 // optional: adult, pediatric, geriatric
    "line_of_therapy": "first-line"        // optional
}

RESPONSE:
{
    "query": "Q1",
    "disorder": "HIV infection",
    "candidates": [
        {
            "generic_name": "NELFINAVIR MESYLATE",
            "brand_names_fda": ["VIRACEPT"],
            "indication_term": "HIV-1 infection",
            "population": "adults and children ≥ 2 years",
            "line_of_therapy": "first-line",
            "combination_required": true,
            "combination_agents": ["antiretroviral agents"],
            "indian_brands": [
                {"brand_name": "Nelficine", "manufacturer": "Cipla", 
                 "strength": "250 mg", "mrp": 450.00}
            ]
        }
    ],
    "evidence": "Based on the FDA label, VIRACEPT is indicated for... [dosage_and_administration, 1.1]",
    "citations": [{"section": "indications_and_usage", "excerpt": "..."}]
}

PSEUDOCODE:
"""

async def query_disorder_to_medications(request: Q1Request) -> Q1Response:
    # Step 1: Map disorder to ICD-10/SNOMED (if free text)
    IF request.disorder matches ICD-10 pattern (e.g., "B20"):
        icd10_code = request.disorder
    ELSE:
        # Lookup in drug_indication table via text search
        icd10_code = None  # will use text matching

    # Step 2: SQL — find all drugs indicated for this disorder
    sql = """
        SELECT DISTINCT d.formulation_id, d.generic_name, d.brand_names,
               di.term, di.icd10, di.population, di.line_of_therapy,
               di.combination_required, di.combination_agents
        FROM drug d
        JOIN drug_indication di USING (formulation_id)
        WHERE ($1::text IS NOT NULL AND di.icd10 = $1)
           OR di.term ILIKE '%' || $2 || '%'
    """
    params = [icd10_code, request.disorder]
    
    IF request.population:
        sql += " AND (di.population = 'any' OR di.population ILIKE '%' || $3 || '%')"
        params.append(request.population)
    
    IF request.line_of_therapy:
        sql += " AND di.line_of_therapy = $4"
        params.append(request.line_of_therapy)
    
    sql += """ ORDER BY 
        CASE di.line_of_therapy 
            WHEN 'first-line' THEN 1 WHEN 'adjunct' THEN 2 
            WHEN 'second-line' THEN 3 ELSE 4 END,
        d.generic_name"""
    
    candidates = await db.fetch(sql, *params)

    # Step 3: Vector search — get evidence chunks for top candidates
    top_ids = [c["formulation_id"] for c in candidates[:10]]
    chunks = await search_chunks(
        query_text=f"indicated for treatment of {request.disorder}",
        formulation_ids=top_ids,
        semantic_types=["indications_and_usage", "clinical_studies"],
        top_k=5
    )

    # Step 4: LLM composition
    composed = await compose_response(
        query_template="Q1",
        patient_context={"disorder": request.disorder, "population": request.population},
        sql_results={"candidates": candidates},
        graph_results={},
        rag_chunks=chunks,
        computed_values={}
    )

    # Step 5: Indian brand mapping for each candidate
    FOR candidate IN candidates:
        candidate["indian_brands"] = await get_indian_brands(db, candidate["formulation_id"])

    # Step 6: Post-checks
    post_check = run_post_checks("Q1", {"candidates": candidates}, composed, {})

    # Step 7: Audit log
    await log_query("Q1", request, candidates, composed, post_check)

    RETURN Q1Response(candidates=candidates, evidence=composed.answer, citations=composed.citations)


# ============================================================================
# ENDPOINT 2: POST /api/v1/query/interaction-check  (Q2)
# ============================================================================
"""
REQUEST:
{
    "drugs": ["Nelficine", "Simvastatin", "Warfarin"],    // Indian brands OR generics
    "adding_drug": "Rifampin"                              // optional: drug being added
}

RESPONSE:
{
    "query": "Q2",
    "resolved_drugs": [
        {"input": "Nelficine", "resolved": "NELFINAVIR MESYLATE", "match_type": "indian_brand_exact",
         "is_fdc": false, "components": []}
    ],
    "interactions": [
        {
            "drug_a": "NELFINAVIR MESYLATE",
            "drug_b": "Simvastatin",
            "severity": "contraindicated",
            "mechanism": "CYP3A4 inhibition",
            "magnitude": "↑AUC 505%",
            "management": "Do not coadminister",
            "subject_substance_role": "active_ingredient",
            "source": "openfda"
        }
    ],
    "pathway_risks": [
        {"drug_a": "X", "drug_b": "Y", "shared_enzyme": "CYP3A4", "risk": "both metabolised by CYP3A4"}
    ],
    "hard_blocks": ["NELFINAVIR + Simvastatin: CONTRAINDICATED"],
    "evidence": "...",
    "citations": [...]
}

PSEUDOCODE:
"""

async def query_interaction_check(request: Q2Request) -> Q2Response:
    # Step 1: Resolve ALL drug names → formulation_ids
    all_drugs = request.drugs + ([request.adding_drug] if request.adding_drug else [])
    resolved = []
    all_formulation_ids = []
    
    FOR drug_name IN all_drugs:
        r = await resolve_drug(db, drug_name)
        resolved.append({"input": drug_name, "resolved": r})
        FOR entry IN r:
            all_formulation_ids.append(entry.formulation_id)
    
    # Step 2: Pairwise SQL interaction lookup
    interactions = []
    pairs = all_unique_pairs(all_formulation_ids)
    
    FOR (a_id, b_id) IN pairs:
        sql = """
            SELECT severity, effect_direction, magnitude, mechanism,
                   clinical_management, subject_substance, subject_substance_role, source
            FROM drug_interaction
            WHERE (subject_formulation_id = $1 
                   AND (partner_rxcui IN (SELECT id_value FROM drug_identifier WHERE formulation_id = $2 AND id_type = 'rxcui')
                        OR partner_drugbank_id IN (SELECT id_value FROM drug_identifier WHERE formulation_id = $2 AND id_type = 'drugbank')
                        OR partner_name ILIKE (SELECT generic_name FROM drug WHERE formulation_id = $2) || '%'))
               OR (subject_formulation_id = $2 
                   AND (partner_rxcui IN (SELECT id_value FROM drug_identifier WHERE formulation_id = $1 AND id_type = 'rxcui')
                        OR partner_drugbank_id IN (SELECT id_value FROM drug_identifier WHERE formulation_id = $1 AND id_type = 'drugbank')
                        OR partner_name ILIKE (SELECT generic_name FROM drug WHERE formulation_id = $1) || '%'))
        """
        rows = await db.fetch(sql, a_id, b_id)
        interactions.extend(rows)
    
    # Step 3: Class-level contraindication check
    FOR (a_id, b_id) IN pairs:
        sql = """
            SELECT c.drug_class, c.reason, c.term
            FROM contraindication c
            WHERE c.formulation_id = $1 
              AND c.kind = 'coadministered_drug'
              AND EXISTS (
                  SELECT 1 FROM drug d 
                  WHERE d.formulation_id = $2 
                    AND c.drug_class = ANY(d.drug_class)
              )
        """
        class_hits = await db.fetch(sql, a_id, b_id)
        interactions.extend(class_hits)

    # Step 4: Neo4j — shared metabolic pathway check
    cypher = """
        MATCH (a:Drug)-[:METABOLISED_BY]->(e:Enzyme)<-[r2:INHIBITS|INDUCES]-(b:Drug)
        WHERE a.formulation_id IN $drug_ids AND b.formulation_id IN $drug_ids AND a <> b
        RETURN a.generic_name as drug_a, b.generic_name as drug_b, 
               e.name as enzyme, type(r2) as effect
    """
    pathway_risks = await neo4j.run(cypher, drug_ids=all_formulation_ids)

    # Step 5: Identify hard blocks
    hard_blocks = [ix for ix in interactions if ix["severity"] == "contraindicated"]

    # Step 6: Vector search for interaction evidence
    chunks = await search_chunks(
        query_text="drug interaction clinical management",
        formulation_ids=all_formulation_ids,
        semantic_types=["drug_interactions", "fact.interaction", "table.interaction"],
        top_k=5
    )

    # Step 7: LLM composition
    composed = await compose_response("Q2", 
        patient_context={"drugs": all_drugs},
        sql_results={"interactions": interactions},
        graph_results={"pathway_risks": pathway_risks},
        rag_chunks=chunks, computed_values={})

    # Step 8: Post-checks (severity floor enforced)
    post_check = run_post_checks("Q2", {"interactions": interactions}, composed, {})
    IF NOT post_check.passed:
        composed = REGENERATE with stricter prompt  # or return raw SQL results

    RETURN Q2Response(...)


# ============================================================================
# ENDPOINT 3: POST /api/v1/query/alternatives  (Q3)
# ============================================================================
"""
REQUEST:
{
    "drug": "Nelficine",
    "patient_meds": ["Rifampin", "Metformin"],
    "reason": "interaction with Rifampin"
}

PSEUDOCODE:
"""

async def query_alternatives(request: Q3Request) -> Q3Response:
    # Step 1: Resolve target drug
    target = await resolve_drug(db, request.drug)
    target_id = target[0].formulation_id
    
    # Step 2: Resolve patient's other meds
    patient_med_ids = []
    FOR med IN request.patient_meds:
        r = await resolve_drug(db, med)
        patient_med_ids.extend([e.formulation_id for e in r])

    # Step 3: Neo4j — find same-class drugs with overlapping indications, 
    #          excluding those with severe interactions with patient meds
    cypher = """
        MATCH (target:Drug {formulation_id: $target_id})
              -[:BELONGS_TO_CLASS]->(c:DrugClass)
              <-[:BELONGS_TO_CLASS]-(alt:Drug)
        WHERE alt <> target
        AND EXISTS((alt)-[:INDICATED_FOR]->(:Indication)<-[:INDICATED_FOR]-(target))
        
        OPTIONAL MATCH (alt)-[ix:INTERACTS_WITH]->(pm:Drug)
        WHERE pm.formulation_id IN $patient_med_ids
        
        WITH alt, c, 
             collect(ix) as all_interactions,
             size([ix IN collect(ix) WHERE ix.severity IN ['contraindicated','major']]) as severe_count
        WHERE severe_count = 0
        
        RETURN alt.formulation_id, alt.generic_name, alt.brand_names,
               c.name as shared_class,
               size(all_interactions) as interaction_count
        ORDER BY interaction_count ASC, alt.generic_name
        LIMIT 10
    """
    alternatives = await neo4j.run(cypher, target_id=target_id, patient_med_ids=patient_med_ids)

    # Step 4: For each alternative, get Indian brands
    FOR alt IN alternatives:
        alt["indian_brands"] = await get_indian_brands(db, alt["formulation_id"])

    # Step 5: Vector search for evidence
    alt_ids = [a["formulation_id"] for a in alternatives[:5]]
    chunks = await search_chunks(
        query_text="indication and clinical efficacy",
        formulation_ids=alt_ids,
        semantic_types=["indications_and_usage"],
        top_k=5
    )

    # Step 6: LLM composition + post-checks
    composed = await compose_response("Q3", ...)
    
    RETURN Q3Response(alternatives=alternatives, evidence=composed.answer)


# ============================================================================
# ENDPOINT 4: POST /api/v1/query/dose-recommendation  (Q4)
# ============================================================================
"""
REQUEST:
{
    "drug": "Nelficine",
    "age": 9,
    "weight_kg": 25.0,
    "sex": "female",
    "renal": "normal",
    "hepatic": "normal",
    "pregnancy": "not_pregnant",
    "current_meds": ["Rifabutin"]
}

PSEUDOCODE:
"""

async def query_dose_recommendation(request: Q4Request) -> Q4Response:
    # Step 1: Resolve drug
    resolved = await resolve_drug(db, request.drug)
    drug_id = resolved[0].formulation_id
    
    # Step 2: SQL — find matching dosing regimen
    regimen = await db.fetchrow("""
        SELECT * FROM dosing_regimen
        WHERE formulation_id = $1
          AND (age_min_years IS NULL OR $2 >= age_min_years)
          AND (age_max_years IS NULL OR $2 <= age_max_years)
          AND (weight_min_kg IS NULL OR $3 >= weight_min_kg)
          AND (weight_max_kg IS NULL OR $3 <= weight_max_kg)
          AND (renal_function IN ($4, 'any'))
          AND (hepatic_function IN ($5, 'any'))
          AND (pregnancy_status IN ($6, 'any'))
        ORDER BY
          (CASE WHEN age_min_years IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN weight_min_kg IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN renal_function != 'any' THEN 1 ELSE 0 END +
           CASE WHEN hepatic_function != 'any' THEN 1 ELSE 0 END) DESC
        LIMIT 1
    """, drug_id, request.age, request.weight_kg, request.renal, request.hepatic, request.pregnancy)
    
    IF NOT regimen:
        RETURN error("No dosing regimen found for this patient profile")
    
    IF regimen["dose_amount"] == "CONTRAINDICATED":
        RETURN hard_block("Drug is contraindicated for this patient population")

    # Step 3: Compute concrete dose
    IF regimen["dose_basis"] == "per_kg":
        raw_dose = regimen["dose_value"] * request.weight_kg
        # Round to nearest available strength
        strengths = await db.fetch(
            "SELECT strength_value FROM available_strength WHERE formulation_id = $1 ORDER BY strength_value",
            drug_id)
        computed_dose = round_to_nearest_strength(raw_dose, [s["strength_value"] for s in strengths])
    ELIF regimen["dose_basis"] == "fixed":
        computed_dose = regimen["dose_value"]
    ELSE:
        computed_dose = regimen["dose_value"]

    # Step 4: Check for dose adjustments from current meds
    adjustments = []
    IF request.current_meds:
        FOR med IN request.current_meds:
            adj = await db.fetch("""
                SELECT dose_amount, administration_notes, adjustment_required_for
                FROM dosing_regimen
                WHERE formulation_id = $1
                  AND $2 = ANY(adjustment_required_for)
            """, drug_id, med)
            IF adj:
                adjustments.extend(adj)

    # Step 5: Get administration timing
    timing = await db.fetchrow(
        "SELECT * FROM administration_timing WHERE formulation_id = $1", drug_id)

    # Step 6: Vector search for dosing evidence
    chunks = await search_chunks(
        query_text="dosage administration dose adjustment",
        formulation_ids=[drug_id],
        semantic_types=["dosage_and_administration", "table.dosing"],
        top_k=5
    )

    # Step 7: Indian brand mapping with computed strength
    indian_brands = await get_indian_brands(db, drug_id, strength_filter=computed_dose)
    IF NOT indian_brands:
        indian_brands = await get_indian_brands(db, drug_id)  # show all strengths

    # Step 8: LLM composition
    composed = await compose_response("Q4", 
        patient_context=request.dict(),
        sql_results={"regimen": regimen, "adjustments": adjustments, "timing": timing},
        graph_results={},
        rag_chunks=chunks,
        computed_values={"computed_dose": computed_dose, "frequency": regimen["frequency"]})

    # Step 9: Post-checks (dose sanity)
    post_check = run_post_checks("Q4", {"regimen": regimen}, composed, 
                                  {"computed_dose": computed_dose})

    RETURN Q4Response(computed_dose=computed_dose, ...)


# ============================================================================
# ENDPOINT 5: POST /api/v1/query/population-approval  (Q5)
# ============================================================================
"""
REQUEST:
{
    "drug": "Viracept",
    "population": "pediatric"       // pediatric, geriatric, pregnant, lactating
}

PSEUDOCODE:
"""

async def query_population_approval(request: Q5Request) -> Q5Response:
    # Step 1: Resolve drug
    resolved = await resolve_drug(db, request.drug)
    drug_id = resolved[0].formulation_id

    # Step 2: SQL lookup
    approval = await db.fetchrow("""
        SELECT status, approved_age_range, pregnancy_category, has_registry, notes
        FROM population_approval
        WHERE formulation_id = $1 AND population = $2
    """, drug_id, request.population)

    # Step 3: Vector search for evidence from relevant section
    section_map = {
        "pediatric": "pediatric_use", "adolescent": "pediatric_use",
        "geriatric": "geriatric_use", "pregnant": "use_in_pregnancy",
        "lactating": "use_in_specific_populations"
    }
    chunks = await search_chunks(
        query_text=f"{request.population} use safety efficacy",
        formulation_ids=[drug_id],
        sections=[section_map.get(request.population, "use_in_specific_populations")],
        top_k=3
    )

    # Step 4: LLM composition
    composed = await compose_response("Q5", ...)

    # Step 5: Indian brands (only if approved)
    indian_brands = []
    IF approval and approval["status"] in ("approved", "benefit_risk"):
        indian_brands = await get_indian_brands(db, drug_id)

    RETURN Q5Response(status=approval["status"], ...)


# ============================================================================
# ENDPOINT 6: POST /api/v1/query/safe-drugs-for-condition  (Q6)
# ============================================================================
"""
REQUEST:
{
    "condition": "HIV infection",
    "patient_meds": ["Rifampin", "Warfarin"]
}

PSEUDOCODE:  This is Q1 + Q2 composed
"""

async def query_safe_drugs_for_condition(request: Q6Request) -> Q6Response:
    # Step 1: Run Q1 to get candidates
    candidates = await _get_candidates_for_disorder(request.condition)

    # Step 2: Resolve patient meds
    patient_med_ids = []
    FOR med IN request.patient_meds:
        r = await resolve_drug(db, med)
        patient_med_ids.extend([e.formulation_id for e in r])

    # Step 3: For each candidate, check interactions with patient meds
    safe_candidates = []
    FOR candidate IN candidates:
        interaction_sql = """
            SELECT severity, COUNT(*) as cnt
            FROM drug_interaction
            WHERE subject_formulation_id = $1
              AND (partner_rxcui IN (SELECT id_value FROM drug_identifier WHERE formulation_id = ANY($2) AND id_type = 'rxcui')
                   OR partner_drugbank_id IN (SELECT id_value FROM drug_identifier WHERE formulation_id = ANY($2) AND id_type = 'drugbank'))
            GROUP BY severity
        """
        severity_counts = await db.fetch(interaction_sql, candidate["formulation_id"], patient_med_ids)
        
        has_severe = any(s["severity"] in ("contraindicated", "major") for s in severity_counts)
        IF NOT has_severe:
            candidate["interaction_count"] = sum(s["cnt"] for s in severity_counts)
            candidate["indian_brands"] = await get_indian_brands(db, candidate["formulation_id"])
            safe_candidates.append(candidate)

    # Step 4: Sort by interaction burden (ascending)
    safe_candidates.sort(key=lambda c: c["interaction_count"])

    # Step 5: LLM composition + post-checks
    # ...

    RETURN Q6Response(safe_candidates=safe_candidates, ...)


# ============================================================================
# ENDPOINT 7: POST /api/v1/query/organ-impairment-dosing  (Q7)
# ============================================================================
"""
REQUEST:
{
    "drug": "Viracept",
    "impairment_type": "hepatic",     // renal or hepatic
    "severity": "mild_impairment"      // mild_impairment, moderate_impairment, severe_impairment, esrd
}

PSEUDOCODE:
"""

async def query_organ_impairment_dosing(request: Q7Request) -> Q7Response:
    resolved = await resolve_drug(db, request.drug)
    drug_id = resolved[0].formulation_id
    
    field = "renal_function" if request.impairment_type == "renal" else "hepatic_function"

    # Step 1: SQL — find regimen for this impairment level
    regimen = await db.fetchrow(f"""
        SELECT * FROM dosing_regimen
        WHERE formulation_id = $1 AND {field} = $2
    """, drug_id, request.severity)

    # Step 2: Fallback — check adjustment_required_for
    IF NOT regimen:
        regimen = await db.fetchrow("""
            SELECT * FROM dosing_regimen
            WHERE formulation_id = $1
              AND $2 = ANY(adjustment_required_for)
        """, drug_id, request.impairment_type)

    # Step 3: Fallback — normal regimen + use_in_specific_populations text
    IF NOT regimen:
        regimen = await db.fetchrow("""
            SELECT * FROM dosing_regimen
            WHERE formulation_id = $1 AND {field} = 'any'
            ORDER BY (CASE WHEN age_group = 'adult' THEN 1 ELSE 2 END)
            LIMIT 1
        """.format(field=field), drug_id)

    # Step 4: Check if CONTRAINDICATED
    IF regimen and regimen["dose_amount"] == "CONTRAINDICATED":
        RETURN hard_block(f"Drug should not be used in {request.severity}")

    # Step 5: Vector search
    chunks = await search_chunks(
        query_text=f"{request.impairment_type} impairment dose adjustment",
        formulation_ids=[drug_id],
        semantic_types=["dosage_and_administration", "use_in_specific_populations"],
        top_k=5
    )

    # Step 6: Indian brands + LLM composition + post-checks
    indian_brands = await get_indian_brands(db, drug_id)
    composed = await compose_response("Q7", ...)
    
    RETURN Q7Response(...)


# ============================================================================
# ENDPOINT 8: POST /api/v1/query/administration-timing  (Q8)
# ============================================================================
"""
REQUEST:
{
    "drug": "Viracept",
    "current_meds": ["Didanosine"]     // optional
}

PSEUDOCODE:
"""

async def query_administration_timing(request: Q8Request) -> Q8Response:
    resolved = await resolve_drug(db, request.drug)
    drug_id = resolved[0].formulation_id

    # Step 1: SQL — get stored timing data
    timing = await db.fetchrow(
        "SELECT * FROM administration_timing WHERE formulation_id = $1", drug_id)

    # Step 2: If current_meds provided, check drug-specific separations
    relevant_separations = []
    IF timing and timing["drug_separations"] and request.current_meds:
        all_separations = json.loads(timing["drug_separations"])
        FOR sep IN all_separations:
            FOR med IN request.current_meds:
                IF med.lower() in sep.get("other_drug", "").lower():
                    relevant_separations.append(sep)

    # Step 3: Also check interaction table for timing-related management
    IF request.current_meds:
        FOR med IN request.current_meds:
            med_resolved = await resolve_drug(db, med)
            timing_interactions = await db.fetch("""
                SELECT partner_name, clinical_management
                FROM drug_interaction
                WHERE subject_formulation_id = $1
                  AND (clinical_management ILIKE '%hour%before%'
                       OR clinical_management ILIKE '%hour%after%'
                       OR clinical_management ILIKE '%separate%'
                       OR clinical_management ILIKE '%empty%stomach%')
                  AND partner_name ILIKE '%' || $2 || '%'
            """, drug_id, med)
            relevant_separations.extend(timing_interactions)

    # Step 4: Vector search (short — timing is mostly structured)
    chunks = await search_chunks(
        query_text="food meal timing administration",
        formulation_ids=[drug_id],
        semantic_types=["dosage_and_administration"],
        top_k=3
    )

    # Step 5: Compose response (may skip LLM if data is clean)
    food_map = {
        "with_food": "Take with food or a meal",
        "empty_stomach": "Take on an empty stomach",
        "either": "May be taken with or without food"
    }
    
    response = {
        "food_requirement": food_map.get(timing["food_requirement"], "Unknown"),
        "food_details": timing.get("food_details"),
        "drug_separations": relevant_separations,
        "indian_brands": await get_indian_brands(db, drug_id)
    }

    # Step 6: LLM only if separations are complex
    IF relevant_separations:
        composed = await compose_response("Q8", ...)
        response["evidence"] = composed.answer

    RETURN Q8Response(**response)


# ============================================================================
# ENDPOINT 9: POST /api/v1/query/pill-burden  (Q9)
# ============================================================================
"""
REQUEST:
{
    "drug": "Viracept",
    "daily_dose_mg": 2500,
    "frequency": "BID"
}

RESPONSE:
{
    "query": "Q9",
    "recommendation": {
        "strength": "625 MG",
        "pills_per_dose": 2,
        "doses_per_day": 2,
        "total_daily_pills": 4,
        "exact_dose_per_admin": 1250.0,
        "wastage_mg": 0.0
    },
    "comparison": [
        {"strength": "250 MG", "pills_per_dose": 5, "total_daily_pills": 10, "wastage": 0},
        {"strength": "625 MG", "pills_per_dose": 2, "total_daily_pills": 4, "wastage": 0}
    ],
    "indian_brands": {
        "625 MG": [{"brand": "Nelficine", "manufacturer": "Cipla", "mrp": 900}],
        "250 MG": [{"brand": "Nelficine", "manufacturer": "Cipla", "mrp": 450}]
    }
}

PSEUDOCODE:  THIS ENDPOINT NEEDS NO LLM — pure computation
"""

async def query_pill_burden(request: Q9Request) -> Q9Response:
    import math
    
    resolved = await resolve_drug(db, request.drug)
    drug_id = resolved[0].formulation_id

    # Step 1: Get available strengths
    strengths = await db.fetch("""
        SELECT DISTINCT strength_value, strength_unit, strength_label
        FROM available_strength
        WHERE formulation_id = $1
        ORDER BY strength_value
    """, drug_id)

    # Also check Indian brand strengths (may have different options)
    indian_strengths = await db.fetch("""
        SELECT DISTINCT strength_value, strength_unit, strength_label
        FROM indian_brand
        WHERE formulation_id = $1 AND strength_value IS NOT NULL
        ORDER BY strength_value
    """, drug_id)

    # Merge and deduplicate
    all_strengths = deduplicate_strengths(strengths + indian_strengths)

    IF NOT all_strengths:
        RETURN error("No strength information available for this drug")

    # Step 2: Compute pill burden for each strength
    freq_map = {"QD": 1, "BID": 2, "TID": 3, "QID": 4, "q8h": 3, "q12h": 2}
    doses_per_day = freq_map.get(request.frequency.upper(), 2)
    dose_per_admin = request.daily_dose_mg / doses_per_day

    comparison = []
    FOR s IN all_strengths:
        pills = math.ceil(dose_per_admin / s["strength_value"])
        total = pills * doses_per_day
        exact = pills * s["strength_value"]
        wastage = exact - dose_per_admin
        comparison.append({
            "strength": s["strength_label"],
            "strength_value": s["strength_value"],
            "pills_per_dose": pills,
            "doses_per_day": doses_per_day,
            "total_daily_pills": total,
            "exact_dose_per_admin": exact,
            "wastage_mg": wastage
        })

    # Step 3: Pick best (lowest total pills, tiebreak: lowest wastage)
    comparison.sort(key=lambda c: (c["total_daily_pills"], c["wastage_mg"]))
    recommendation = comparison[0]

    # Step 4: Get Indian brands per strength
    brands_by_strength = {}
    FOR s IN all_strengths:
        brands = await get_indian_brands(db, drug_id, strength_filter=s["strength_value"])
        brands_by_strength[s["strength_label"]] = brands

    # NO LLM call needed — this is pure computation
    # NO post-checks needed — no LLM output to validate

    RETURN Q9Response(
        recommendation=recommendation,
        comparison=comparison,
        indian_brands=brands_by_strength
    )