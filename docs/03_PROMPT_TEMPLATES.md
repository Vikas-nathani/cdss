# ============================================================================
# CDSS LLM PROMPT TEMPLATES
# ============================================================================
# Used by the Response Composer (app/core/response_composer.py)
# Each query template has a SYSTEM prompt and a USER prompt template.
# The composition model (Qwen2.5-7B-Instruct) receives these via vLLM.
# ============================================================================

# ============================================================================
# SHARED SYSTEM PROMPT (used by all query templates)
# ============================================================================

SYSTEM_PROMPT_BASE = """You are a Clinical Decision Support assistant. Your role is to present retrieved clinical evidence to medical practitioners clearly and accurately.

RULES:
1. Use ONLY the retrieved evidence provided below. Never add information from your training.
2. Every factual claim must cite its source using [section_name] format.
3. If evidence is missing or insufficient for any part, say so explicitly.
4. Never compute or estimate drug doses — use only the computed values provided.
5. Never downgrade interaction severity from what the data shows.
6. Present information in a structured, scannable format suitable for clinical use.
7. Use Indian brand names alongside generic names when Indian brand data is provided.
8. Keep language professional and concise — this is for practicing clinicians."""

# ============================================================================
# Q1: Disorder → Medications
# ============================================================================

PROMPT_Q1 = """A clinician asks: Which medications treat {disorder}?
{population_filter}

RETRIEVED CANDIDATE DRUGS:
{sql_results}

RETRIEVED EVIDENCE:
{evidence}

TASK:
1. List each candidate drug with its FDA-approved indication [cite section]
2. Group by line of therapy (first-line, second-line, adjunct) if available
3. Note any combination requirements (must be used with other agents)
4. For each drug, list available Indian brands with strengths and manufacturers
5. If evidence is limited, state that clearly

Format as a structured clinical summary. Start with the strongest recommendation."""

# ============================================================================
# Q2: Interaction Check
# ============================================================================

PROMPT_Q2 = """A clinician asks: Do these drugs interact?

PATIENT'S MEDICATION LIST: {drugs}

DETECTED INTERACTIONS (from structured database):
{sql_results}

SHARED METABOLIC PATHWAY RISKS (from graph database):
{graph_results}

RETRIEVED EVIDENCE:
{evidence}

TASK:
1. List ALL detected interactions, ordered by severity: contraindicated first, then major, moderate, minor
2. For each interaction:
   - State the two drugs involved
   - Severity level [cite source]
   - Mechanism if known (e.g., CYP3A4 inhibition) [cite source]
   - Magnitude if known (e.g., ↑AUC 505%) [cite source]
   - Clinical recommendation (avoid, adjust dose, monitor, separate timing) [cite source]
   - Whether the interaction involves the active ingredient or an excipient
3. For shared metabolic pathway risks, explain the potential for indirect interaction
4. If no interactions were found, state that explicitly

CRITICAL: If ANY interaction has severity "contraindicated", start your response with a clear WARNING block. Do NOT suggest it is safe to proceed."""

# ============================================================================
# Q3: Alternatives
# ============================================================================

PROMPT_Q3 = """A clinician asks: What are alternatives to {target_drug}?
Reason for switch: {reason}
Patient's other medications: {patient_meds}

ALTERNATIVE CANDIDATES (same therapeutic class, compatible with patient meds):
{sql_results}

RETRIEVED EVIDENCE:
{evidence}

TASK:
1. List each alternative drug with:
   - Generic name and therapeutic class
   - How it compares to the original drug (shared indication)
   - Number of interactions with patient's current medications (fewer = better)
   - Available Indian brands with strengths and pricing
2. Rank alternatives: fewest interactions first
3. Note any trade-offs (different side effect profile, different dosing frequency)
4. If no suitable alternatives were found, state that explicitly"""

# ============================================================================
# Q4: Dose Recommendation
# ============================================================================

PROMPT_Q4 = """A clinician asks: What dose of {drug} for this patient?

PATIENT PROFILE:
- Age: {age} years, Sex: {sex}, Weight: {weight_kg} kg
- Renal function: {renal}
- Hepatic function: {hepatic}
- Pregnancy status: {pregnancy}
- Current medications: {current_meds}

MATCHED DOSING REGIMEN (from structured database):
{sql_results}

COMPUTED DOSE: {computed_dose} mg {frequency}
DOSE ADJUSTMENTS NEEDED: {adjustments}
ADMINISTRATION TIMING: {timing}

RETRIEVED EVIDENCE:
{evidence}

AVAILABLE INDIAN BRANDS: {indian_brands}

TASK:
1. State the recommended dose clearly: "{computed_dose} mg {frequency}" [cite dosing section]
2. Explain why this regimen was selected for this patient's profile [cite section]
3. List any dose adjustments triggered by current medications [cite section]
4. Administration instructions: food requirements, timing separations [cite section]
5. Maximum daily dose if stated in the label [cite section]
6. Suggest the specific Indian brand and number of tablets per dose

CRITICAL: The computed dose is {computed_dose} mg. Do NOT state a different dose value. The dose was computed from structured data and is authoritative."""

# ============================================================================
# Q5: Population Approval
# ============================================================================

PROMPT_Q5 = """A clinician asks: Is {drug} approved for {population} patients?

STRUCTURED APPROVAL DATA:
{sql_results}

RETRIEVED EVIDENCE:
{evidence}

TASK:
1. State the approval status clearly: APPROVED / STUDIED BUT NOT FORMALLY APPROVED / NOT STUDIED / CONTRAINDICATED / BENEFIT-RISK
2. If approved for pediatric use, state the approved age range [cite section]
3. If pregnancy-related, note the pregnancy category and whether a registry exists [cite section]
4. Summarize the key evidence supporting the status [cite section]
5. If "studied but not approved" or "not studied", explain what the label says about available data
6. List available Indian brands if the drug is approved for this population"""

# ============================================================================
# Q6: Safe Drugs for Condition
# ============================================================================

PROMPT_Q6 = """A clinician asks: What drugs treat {condition} that are safe with my patient's current medications?

PATIENT'S CURRENT MEDICATIONS: {patient_meds}

SAFE CANDIDATES (indicated for condition, no severe interactions with current meds):
{sql_results}

EXCLUDED DRUGS (had contraindicated or major interactions):
{excluded}

RETRIEVED EVIDENCE:
{evidence}

TASK:
1. List safe candidates, ordered by total interaction count (fewest first)
2. For each candidate:
   - Generic name [cite indication section]
   - Number and severity of interactions with patient's current meds
   - Available Indian brands
3. Note which drugs were EXCLUDED and why (which current med caused the exclusion)
4. If no safe candidates exist, state that explicitly and suggest the clinician review the medication list"""

# ============================================================================
# Q7: Organ Impairment Dosing
# ============================================================================

PROMPT_Q7 = """A clinician asks: What dose adjustment is needed for {drug} in {severity} {impairment_type} impairment?

DOSING DATA FOR THIS IMPAIRMENT LEVEL:
{sql_results}

RETRIEVED EVIDENCE:
{evidence}

TASK:
1. State clearly whether dose adjustment is required [cite section]
2. If yes: state the adjusted dose [cite section]
3. If the drug is CONTRAINDICATED for this impairment level, state that as a clear WARNING [cite section]
4. If no specific data exists for this impairment level, state that the label does not provide guidance
5. Note any monitoring recommendations [cite section]
6. List available Indian brands at the adjusted dose strength"""

# ============================================================================
# Q8: Administration Timing
# ============================================================================

PROMPT_Q8 = """A clinician asks: How should {drug} be taken? (food, timing, separations)

Patient is also taking: {current_meds}

STRUCTURED TIMING DATA:
- Food requirement: {food_requirement}
- Drug separations: {drug_separations}

RETRIEVED EVIDENCE:
{evidence}

TASK:
1. State the food requirement clearly [cite section]
2. For each current medication that requires timing separation:
   - State the specific timing (e.g., "take 1 hour after or 2 hours before") [cite section]
   - Explain the reason if available
3. Any other administration notes (crushing, dissolving, mixing) [cite section]
4. Keep the answer practical and actionable"""

# ============================================================================
# Q9: Pill Burden — NO LLM PROMPT NEEDED
# ============================================================================
# Q9 is pure computation. The response is structured data, not natural language.
# No LLM call is made for Q9.

# ============================================================================
# PASS 2 EXTRACTION PROMPTS (used during batch ingestion, not query-time)
# ============================================================================
# These use Qwen2.5-72B for Tier 1, Qwen2.5-7B for Tier 2.
# See pass2_extractors.py for the full prompt builders.
# ============================================================================

EXTRACTION_SYSTEM = """You are a clinical data extraction assistant. Extract structured data from FDA drug labels. Return ONLY valid JSON. No markdown, no explanation, no backticks."""

EXTRACTION_INDICATION = """Extract every distinct indication from this FDA label section for {drug_name}.

LABEL TEXT:
{text}

For each indication, return a JSON object:
{{"term": "condition as written", "icd10": "code or null", "snomed": "code or null", "population": "who or any", "line_of_therapy": "first-line|second-line|adjunct|salvage|unspecified", "combination_required": true/false, "combination_agents": []}}

Return a JSON array."""

EXTRACTION_DRUG_CLASS = """Classify {drug_name}:

MECHANISM: {mechanism}
INDICATIONS: {indications}

Return JSON: {{"pharmacologic_class": [], "therapeutic_class": [], "mechanism_class": [], "atc_code": null}}"""

EXTRACTION_SEVERITY = """Classify severity of each interaction from its management text.

Levels: contraindicated, major, moderate, minor, unknown

{interactions_json}

Return JSON array: [{{"interaction_id": "...", "severity": "...", "mechanism": "..." or null}}]"""

EXTRACTION_DOSING = """Extract dosing regimens for {drug_name} (strengths: {strengths}).

TEXT: {dosage_text}
SUBSECTIONS: {subsections}
TABLES: {tables}

For each regimen return:
{{"population": {{"age_group": "...", "age_min_years": N, "age_max_years": N, "weight_min_kg": N, "weight_max_kg": N, "sex": "any", "pregnancy_status": "any", "renal_function": "any", "hepatic_function": "any"}}, "route": "...", "dose_amount": "...", "dose_value": N, "dose_unit": "...", "dose_basis": "fixed|per_kg|per_m2|titrated", "frequency": "...", "max_daily_dose": "...", "duration": "...", "administration_notes": "...", "adjustment_required_for": []}}

Create SEPARATE entries for adult vs pediatric vs renal/hepatic impairment. If contraindicated for a population, set dose_amount="CONTRAINDICATED".
Return a JSON array."""

EXTRACTION_POPULATION = """Analyze population-specific FDA label sections for {drug_name}:

{sections_json}

Return JSON:
{{"pediatric": {{"status": "approved|studied_not_approved|not_studied|contraindicated", "approved_age_range": "...", "notes": "..."}}, "adolescent": {{...}}, "geriatric": {{...}}, "pregnant": {{"status": "...", "pregnancy_category": "A|B|C|D|X", "has_registry": true/false, "notes": "..."}}, "lactating": {{...}}}}"""