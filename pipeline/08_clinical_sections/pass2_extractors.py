"""
pass2_extractors.py
-------------------
Structured extraction pass that converts narrative text in the unified record
into queryable facts in structured_facts. Each extractor targets a specific
set of CDSS query templates.

Extractors are ordered by dependency:
  1. strength_extractor          — regex, no LLM    (unblocks Q9)
  2. indication_extractor        — LLM required     (unblocks Q1, Q3, Q5, Q6)
  3. drug_class_extractor        — LLM + lookup     (unblocks Q3)
  4. interaction_enricher        — LLM + regex       (unblocks Q2, Q6)
  5. dosing_regimen_extractor    — LLM required      (unblocks Q4, Q7)
  6. population_approval_extractor — LLM             (unblocks Q5)
  7. administration_timing_extractor — regex + LLM   (unblocks Q8)
  8. adverse_event_extractor     — regex + LLM       (future)
  9. pharmacology_extractor      — regex + LLM       (future)

Each extractor:
  - Takes a unified record dict
  - Mutates structured_facts in place
  - Returns a log of what was extracted (for audit)

For extractors marked "LLM required", the function builds the prompt and
returns it. The caller is responsible for sending it to the LLM and parsing
the response. This keeps the extractors testable without an API key.
"""

import json
import re
from typing import Optional


# ============================================================================
# 1. STRENGTH EXTRACTOR (regex only — no LLM)
#    Unblocks: Q9 (pill burden minimization)
# ============================================================================

def extract_strengths(record: dict) -> dict:
    """
    Extracts strength_label and strength_value from RxNorm formulation names.
    Also populates product.skus[].strength_label by matching NDC to RxNorm.
    """
    log = {"extractor": "strength", "extracted": []}

    # Parse strengths from RxNorm names
    strength_map = {}  # rxcui -> {value, unit, label}
    for cf in record.get("rxnorm", {}).get("clinical_formulations", []):
        name = cf.get("name", "")
        match = re.search(r'(\d+(?:\.\d+)?)\s*(MG|MCG|ML|MG/ML|MCG/ML|%)', name, re.I)
        if match:
            val = float(match.group(1))
            unit = match.group(2).upper()
            label = f"{match.group(1)} {unit}"
            cf["strength_value"] = val
            cf["strength_unit"] = unit
            cf["strength_label"] = label
            strength_map[cf.get("rxcui")] = {"value": val, "unit": unit, "label": label}
            log["extracted"].append({"rxcui": cf.get("rxcui"), "strength": label})

    # Propagate to product SKUs via NDC -> RxNorm mapping
    # RxNorm rxcuis are in identifiers; match strengths to SKUs by dosage_form + deduction
    available_strengths = sorted(set(s["value"] for s in strength_map.values()))
    if available_strengths and record.get("product", {}).get("skus"):
        skus = record["product"]["skus"]
        if len(skus) == len(available_strengths):
            # Common case: one SKU per strength
            for sku, strength in zip(skus, available_strengths):
                unit = next(iter(strength_map.values()))["unit"]
                sku["strength_label"] = f"{int(strength) if strength == int(strength) else strength} {unit}"
        elif len(available_strengths) > 0:
            # Assign lowest available strength to first SKU, etc.
            for i, sku in enumerate(skus):
                idx = min(i, len(available_strengths) - 1)
                unit = next(iter(strength_map.values()))["unit"]
                sku["strength_label"] = f"{int(available_strengths[idx]) if available_strengths[idx] == int(available_strengths[idx]) else available_strengths[idx]} {unit}"

    # Add available_strengths to structured_facts for Q9 computation
    if "structured_facts" not in record:
        record["structured_facts"] = {}
    record["structured_facts"]["available_strengths"] = [
        {"value": s["value"], "unit": s["unit"], "label": s["label"],
         "rxcui": rxcui}
        for rxcui, s in strength_map.items()
    ]

    log["available_strengths"] = available_strengths
    return log


# ============================================================================
# 2. INDICATION EXTRACTOR (LLM required)
#    Unblocks: Q1, Q3, Q5, Q6
# ============================================================================

def build_indication_extraction_prompt(record: dict) -> dict:
    """
    Builds the LLM prompt for extracting structured indications from the
    indications_and_usage narrative.

    Returns: {prompt, system, parse_instructions}
    """
    text = (record.get("clinical", {}).get("indications_and_usage", {}) or {}).get("text", "")
    drug_name = record.get("drug", {}).get("generic_name", "")
    brand = (record.get("drug", {}).get("brand_names") or [""])[0]

    if not text:
        return {"prompt": None, "reason": "no indications text"}

    system = """You are a clinical data extraction assistant. Extract structured indication data from FDA drug labels.
Return ONLY a JSON array. No markdown, no explanation, no backticks."""

    prompt = f"""Extract every distinct indication from this FDA label section for {drug_name} ({brand}).

LABEL TEXT:
{text}

For each indication, return a JSON object with these fields:
- "term": the condition/disease as written in the label
- "icd10": best-match ICD-10-CM code (null if uncertain)
- "snomed": best-match SNOMED CT concept ID (null if uncertain)
- "population": patient population if specified (e.g., "adults", "pediatric 2-13y", "treatment-naive"), or "any" if not restricted
- "line_of_therapy": one of "first-line", "second-line", "adjunct", "salvage", "unspecified"
- "combination_required": true if the label says it must be used with other agents, false otherwise
- "combination_agents": list of required combination agents if applicable, else []

Return a JSON array of these objects. If there is only one indication, return an array with one object."""

    return {
        "system": system,
        "prompt": prompt,
        "target_field": "structured_facts.indications",
        "parse_instructions": "Parse JSON array. Each element maps directly to a structured_facts.indications entry. Add source_span with source='openfda', section_code='indications_and_usage'."
    }


# ============================================================================
# 3. DRUG CLASS EXTRACTOR (LLM + lookup)
#    Unblocks: Q3 (alternatives)
# ============================================================================

def build_drug_class_extraction_prompt(record: dict) -> dict:
    """
    Extracts drug class from mechanism_of_action + indications + description.
    """
    moa = (record.get("clinical", {}).get("mechanism_of_action", {}) or {}).get("text", "")
    desc = record.get("drug", {}).get("mechanism_of_action", "") or ""
    indications = (record.get("clinical", {}).get("indications_and_usage", {}) or {}).get("text", "")
    drug_name = record.get("drug", {}).get("generic_name", "")

    system = "You are a pharmacology classification assistant. Return ONLY a JSON object."

    prompt = f"""Classify the drug {drug_name} based on the following label text.

MECHANISM OF ACTION: {moa or desc}
INDICATIONS: {indications}

Return a JSON object:
{{
  "pharmacologic_class": ["<primary class>", "<secondary class if any>"],
  "therapeutic_class": ["<therapeutic class>"],
  "mechanism_class": ["<mechanism-based class, e.g. 'HIV-1 protease inhibitor'>"],
  "atc_code": "<ATC code if you know it, else null>"
}}"""

    return {
        "system": system,
        "prompt": prompt,
        "target_field": "drug.drug_class + drug.atc_codes",
        "parse_instructions": "Merge pharmacologic_class + therapeutic_class + mechanism_class into drug.drug_class (deduplicated). Set drug.atc_codes from atc_code."
    }


# ============================================================================
# 4. INTERACTION ENRICHER (regex + LLM)
#    Unblocks: Q2 (interaction severity/magnitude), Q6 (severity filter)
# ============================================================================

def enrich_interactions_from_tables(record: dict) -> dict:
    """
    Deterministic pass: parses openFDA interaction tables (Tables 12, 13 in the
    sample) to extract magnitude (↑/↓ percentages) and direction for each
    drug pair. Also classifies severity from contraindications table.

    This runs before the LLM pass and populates what it can deterministically.
    """
    log = {"extractor": "interaction_enricher_tables", "enriched": 0}

    # Build a lookup from contraindications table: drug name -> contraindicated
    contraindicated_drugs = set()
    for c in record.get("structured_facts", {}).get("contraindications", []):
        for name in re.split(r'[,;]', c.get("term", "")):
            contraindicated_drugs.add(name.strip().lower())

    # Parse interaction tables for magnitude
    magnitude_lookup = {}  # partner_name_lower -> {auc, cmax, cmin, direction}
    for table in record.get("label_tables", []):
        if table.get("semantic_type") not in ("interaction", "pharmacokinetics"):
            continue
        caption = table.get("caption", "").lower()
        if "drug interaction" not in caption and "pharmacokinetic" not in caption:
            continue

        for row in table.get("rows", []):
            if isinstance(row, list) and len(row) >= 4:
                drug_name = str(row[0]).strip()
                if not drug_name or drug_name.startswith(('HIV', 'Nucleoside', 'Non-nucleoside',
                                                          'Anti-', 'HMG-', 'Other')):
                    continue  # header rows
                # Look for ↑/↓ patterns in remaining columns
                for col in row[3:]:
                    col_str = str(col)
                    m = re.search(r'([↑↓↔])\s*(\d+)%?\s*\(', col_str)
                    if m:
                        direction = {"↑": "increase", "↓": "decrease", "↔": "no_change"}[m.group(1)]
                        pct = m.group(2)
                        magnitude_lookup[drug_name.lower().split()[0]] = {
                            "direction": direction,
                            "magnitude_raw": col_str.strip(),
                        }
                        break

            elif isinstance(row, dict):
                for key in ['Coadministered Drug', 'Drug Name', '']:
                    drug_name = str(row.get(key, '')).strip()
                    if drug_name:
                        break
                auc = str(row.get('AUC', ''))
                m = re.search(r'([↑↓↔])\s*(\d+)?', auc)
                if m and drug_name:
                    direction = {"↑": "increase", "↓": "decrease", "↔": "no_change"}[m.group(1)]
                    magnitude_lookup[drug_name.lower().split()[0]] = {
                        "direction": direction,
                        "magnitude_raw": auc.strip(),
                    }

    # Apply to existing interactions
    for ix in record.get("structured_facts", {}).get("interactions", []):
        partner_name = (ix.get("partner", {}).get("name") or "").lower()

        # Severity from contraindications
        if partner_name in contraindicated_drugs:
            ix["severity"] = "contraindicated"
            log["enriched"] += 1

        # Magnitude from tables
        partner_first = partner_name.split()[0] if partner_name else ""
        if partner_first in magnitude_lookup:
            mag = magnitude_lookup[partner_first]
            ix["effect_direction"] = mag["direction"]
            ix["magnitude"] = mag["magnitude_raw"]
            log["enriched"] += 1

    return log


def build_interaction_severity_prompt(record: dict) -> dict:
    """
    For interactions still at severity=unknown after the table pass,
    build an LLM prompt to classify severity from the clinical management text.
    """
    unknowns = [
        ix for ix in record.get("structured_facts", {}).get("interactions", [])
        if ix.get("severity") == "unknown" and ix.get("clinical_management")
    ]

    if not unknowns:
        return {"prompt": None, "reason": "no unknown-severity interactions"}

    # Batch up to 20 at a time
    batch = unknowns[:20]
    items = []
    for ix in batch:
        items.append({
            "interaction_id": ix["interaction_id"],
            "partner": ix["partner"]["name"],
            "management": ix.get("clinical_management", "")[:300]
        })

    system = "You are a drug interaction severity classifier. Return ONLY a JSON array."

    prompt = f"""Classify the severity of each drug interaction based on the clinical management text.

Severity levels:
- "contraindicated": must never be used together
- "major": serious risk, avoid or use only with close monitoring
- "moderate": may need dose adjustment or monitoring
- "minor": minimal clinical significance
- "unknown": insufficient information to classify

Interactions to classify:
{json.dumps(items, indent=2)}

Return a JSON array where each element has:
- "interaction_id": the ID from input
- "severity": one of the severity levels above
- "mechanism": brief mechanism if inferable (e.g., "CYP3A4 inhibition", "reduced absorption"), else null"""

    return {
        "system": system,
        "prompt": prompt,
        "target_field": "structured_facts.interactions[].severity + mechanism",
        "parse_instructions": "Match by interaction_id and update severity + mechanism fields.",
        "batch_size": len(batch),
        "remaining": len(unknowns) - len(batch),
    }


# ============================================================================
# 5. DOSING REGIMEN EXTRACTOR (LLM required)
#    Unblocks: Q4 (dose for patient), Q7 (renal/hepatic adjustment)
# ============================================================================

def build_dosing_extraction_prompt(record: dict) -> dict:
    """
    Extracts structured dosing regimens from the dosage_and_administration
    narrative + tables.
    """
    dosage_section = record.get("clinical", {}).get("dosage_and_administration", {}) or {}
    text = dosage_section.get("text", "")
    subsections = dosage_section.get("subsections", [])

    # Also include dosing tables as context
    dosing_tables = [t for t in record.get("label_tables", []) if t.get("semantic_type") == "dosing"]
    table_text = ""
    for t in dosing_tables:
        table_text += f"\nTABLE: {t.get('caption','')}\n"
        for row in t.get("rows", [])[:10]:
            table_text += f"  {row}\n"

    drug_name = record.get("drug", {}).get("generic_name", "")
    strengths = record.get("structured_facts", {}).get("available_strengths", [])
    strength_text = ", ".join(s["label"] for s in strengths) if strengths else "unknown"

    system = "You are a clinical dosing data extraction assistant. Return ONLY a JSON array."

    prompt = f"""Extract every distinct dosing regimen for {drug_name} (available strengths: {strength_text}).

DOSAGE AND ADMINISTRATION TEXT:
{text}

SUBSECTIONS:
{json.dumps([{"title": s.get("title",""), "text": s.get("text","")[:500]} for s in subsections], indent=2)}

DOSING TABLES:
{table_text}

For each distinct regimen (different population, dose, or frequency), return:
{{
  "population": {{
    "age_group": "neonate|infant|pediatric|adolescent|adult|geriatric|any",
    "age_min_years": <number or null>,
    "age_max_years": <number or null>,
    "weight_min_kg": <number or null>,
    "weight_max_kg": <number or null>,
    "sex": "any|male|female",
    "pregnancy_status": "any|pregnant|not_pregnant|lactating",
    "renal_function": "any|normal|mild_impairment|moderate_impairment|severe_impairment|esrd",
    "hepatic_function": "any|normal|mild_impairment|moderate_impairment|severe_impairment"
  }},
  "route": "oral|iv|im|sc|topical|...",
  "dose_amount": "1250 mg" or "45-55 mg/kg",
  "dose_value": <number>,
  "dose_unit": "mg|mcg|ml|...",
  "dose_basis": "fixed|per_kg|per_m2|titrated",
  "frequency": "BID|TID|QD|q8h|...",
  "max_daily_dose": "<value> or null",
  "duration": "<duration or null>",
  "administration_notes": "with food|on empty stomach|...",
  "adjustment_required_for": ["list of conditions requiring dose change"]
}}

Important:
- Create SEPARATE entries for adult vs pediatric vs renal/hepatic impairment regimens
- Include weight-based pediatric dosing as dose_basis="per_kg"
- If the label says "should not be used" for a population, still create an entry with dose_amount="CONTRAINDICATED"
- Extract max_daily_dose when stated"""

    return {
        "system": system,
        "prompt": prompt,
        "target_field": "structured_facts.dosing_regimens",
        "parse_instructions": "Replace existing dosing_regimens array entirely. Add source_span with source='merged', section_code='dosage_and_administration'."
    }


# ============================================================================
# 6. POPULATION APPROVAL EXTRACTOR (LLM)
#    Unblocks: Q5 (is drug X approved for population Y?)
# ============================================================================

def build_population_approval_prompt(record: dict) -> dict:
    """
    Extracts a structured population_approval map from the use_in_specific_populations
    sections. This is a simple boolean + notes structure for quick UI display.
    """
    sections = {}
    for key in ["pediatric_use", "geriatric_use", "use_in_pregnancy", "use_in_specific_populations"]:
        payload = record.get("clinical", {}).get(key, {})
        if payload and payload.get("text"):
            sections[key] = payload["text"][:1500]

    if not sections:
        return {"prompt": None, "reason": "no population sections"}

    drug_name = record.get("drug", {}).get("generic_name", "")

    system = "You are a clinical label interpretation assistant. Return ONLY a JSON object."

    prompt = f"""Analyze the population-specific sections of the FDA label for {drug_name} and determine approval/safety status for each population.

LABEL SECTIONS:
{json.dumps(sections, indent=2)}

Return a JSON object:
{{
  "pediatric": {{
    "status": "approved|studied_not_approved|not_studied|contraindicated",
    "approved_age_range": "2-13 years" or null,
    "notes": "brief summary of what the label says"
  }},
  "adolescent": {{
    "status": "approved|studied_not_approved|not_studied|contraindicated",
    "approved_age_range": "13-17 years" or null,
    "notes": "..."
  }},
  "geriatric": {{
    "status": "approved|studied_not_approved|not_studied|contraindicated",
    "notes": "..."
  }},
  "pregnant": {{
    "status": "approved|benefit_risk|contraindicated|not_studied",
    "pregnancy_category": "A|B|C|D|X or null if not stated",
    "has_registry": true/false,
    "notes": "..."
  }},
  "lactating": {{
    "status": "approved|not_recommended|contraindicated|not_studied",
    "notes": "..."
  }}
}}

Use "approved" only if the label explicitly says the drug is indicated/safe/effective for that population.
Use "studied_not_approved" if studies were done but data was insufficient.
Use "not_studied" if the label says no adequate studies exist.
Use "contraindicated" only if explicitly stated."""

    return {
        "system": system,
        "prompt": prompt,
        "target_field": "structured_facts.population_approval",
        "parse_instructions": "Store the entire object as structured_facts.population_approval. This is a NEW field not in the original schema — add it.",
        "schema_addition": {
            "population_approval": {
                "type": "object",
                "properties": {
                    "pediatric":  {"type": "object"},
                    "adolescent": {"type": "object"},
                    "geriatric":  {"type": "object"},
                    "pregnant":   {"type": "object"},
                    "lactating":  {"type": "object"},
                }
            }
        }
    }


# ============================================================================
# 7. ADMINISTRATION TIMING EXTRACTOR (regex + LLM)
#    Unblocks: Q8 (food/timing/separation)
# ============================================================================

def extract_administration_timing(record: dict) -> dict:
    """
    Deterministic extraction of food/timing requirements from narrative.
    Falls back to LLM only if regex can't determine the answer.
    """
    log = {"extractor": "administration_timing", "extracted": {}}

    # Collect text from relevant sections
    texts = {}
    for section in ["dosage_and_administration", "drug_interactions", "information_for_patients"]:
        payload = record.get("clinical", {}).get(section, {})
        if payload and payload.get("text"):
            texts[section] = payload["text"]

    all_text = " ".join(texts.values())

    # 1. Food requirement
    food_timing = None
    if re.search(r'(taken?|give[n]?|administer)\s+with\s+(a\s+)?(food|meal)', all_text, re.I):
        food_timing = "with_food"
    elif re.search(r'(empty|fasting)\s+stomach', all_text, re.I):
        food_timing = "empty_stomach"
    elif re.search(r'without\s+regard\s+to\s+(food|meal)', all_text, re.I):
        food_timing = "either"

    # 2. Drug separation requirements
    separations = []
    patterns = [
        (r'(\w+)\s+should\s+be\s+(?:given|taken|administered)\s+(?:at\s+least\s+)?(\d+)\s+hours?\s+(before|after)\s+(\w+)', "drug_separation"),
        (r'(\d+)\s+hours?\s+(before|after)\s+(\w+)', "time_separation"),
    ]
    for pattern, ptype in patterns:
        for match in re.finditer(pattern, all_text, re.I):
            separations.append({
                "type": ptype,
                "match": match.group(),
                "context": all_text[max(0, match.start()-50):match.end()+50]
            })

    # Store results
    timing = {
        "food_requirement": food_timing,
        "food_requirement_source": "regex",
        "drug_separations": separations,
        "raw_matches": len(separations),
    }

    # Update dosing regimens with food info
    for regimen in record.get("structured_facts", {}).get("dosing_regimens", []):
        if food_timing and not regimen.get("administration_notes"):
            notes_map = {
                "with_food": "Take with food/meal",
                "empty_stomach": "Take on empty stomach",
                "either": "May be taken without regard to food",
            }
            regimen["administration_notes"] = notes_map.get(food_timing, "")

    record.setdefault("structured_facts", {})["administration_timing"] = timing
    log["extracted"] = timing
    return log


def build_timing_llm_prompt(record: dict) -> Optional[dict]:
    """
    If regex extraction was inconclusive, build an LLM prompt for food/timing.
    """
    timing = record.get("structured_facts", {}).get("administration_timing", {})
    if timing.get("food_requirement") and timing.get("drug_separations"):
        return None  # regex was sufficient

    dosage_text = (record.get("clinical", {}).get("dosage_and_administration", {}) or {}).get("text", "")[:2000]
    interactions_text = (record.get("clinical", {}).get("drug_interactions", {}) or {}).get("text", "")[:2000]
    drug_name = record.get("drug", {}).get("generic_name", "")

    system = "You are a clinical administration timing extractor. Return ONLY a JSON object."

    prompt = f"""From the FDA label for {drug_name}, extract all administration timing requirements.

DOSAGE AND ADMINISTRATION:
{dosage_text}

DRUG INTERACTIONS:
{interactions_text}

Return:
{{
  "food_requirement": "with_food|empty_stomach|either|unknown",
  "food_details": "e.g., 'take with a meal; food increases absorption 2-5x'",
  "drug_separations": [
    {{
      "other_drug": "drug name",
      "separation_hours": <number>,
      "timing": "before|after|either_direction",
      "reason": "brief reason"
    }}
  ],
  "other_timing_notes": "any other timing requirements"
}}"""

    return {
        "system": system,
        "prompt": prompt,
        "target_field": "structured_facts.administration_timing",
        "parse_instructions": "Merge with existing administration_timing, overwriting food_requirement if it was null."
    }


# ============================================================================
# ORCHESTRATOR: run all extractors in order
# ============================================================================

def run_deterministic_extractors(record: dict) -> list:
    """
    Runs all regex/deterministic extractors. Returns list of logs.
    No LLM calls — these are fast and testable.
    """
    logs = []
    logs.append(extract_strengths(record))
    logs.append(enrich_interactions_from_tables(record))
    logs.append(extract_administration_timing(record))
    return logs


def build_all_llm_prompts(record: dict) -> list:
    """
    Builds all LLM prompts needed for this record.
    Returns list of {system, prompt, target_field, parse_instructions}.
    The caller sends each to the LLM and applies the result.
    """
    prompts = []
    for builder in [
        build_indication_extraction_prompt,
        build_drug_class_extraction_prompt,
        build_interaction_severity_prompt,
        build_dosing_extraction_prompt,
        build_population_approval_prompt,
        build_timing_llm_prompt,
    ]:
        result = builder(record)
        if result and result.get("prompt"):
            prompts.append(result)
    return prompts


# ============================================================================
# DEMO: run deterministic pass on sample
# ============================================================================

if __name__ == "__main__":
    import sys
    src = sys.argv[1] if len(sys.argv) > 1 else "unified_sample.json"

    with open(src) as f:
        record = json.load(f)

    print("Running deterministic extractors...")
    logs = run_deterministic_extractors(record)
    for log in logs:
        print(f"\n  {log['extractor']}:")
        if 'extracted' in log:
            extracted = log['extracted']
            if isinstance(extracted, dict):
                for k, v in extracted.items():
                    print(f"    {k}: {v}")
            elif isinstance(extracted, list):
                print(f"    {len(extracted)} items")
        if 'enriched' in log:
            print(f"    enriched {log['enriched']} interactions")
        if 'available_strengths' in log:
            print(f"    strengths: {log['available_strengths']}")

    print("\n\nBuilding LLM prompts...")
    prompts = build_all_llm_prompts(record)
    for p in prompts:
        print(f"\n  Target: {p['target_field']}")
        print(f"  Prompt length: {len(p.get('prompt',''))} chars")
        if p.get('batch_size'):
            print(f"  Batch size: {p['batch_size']}, remaining: {p.get('remaining',0)}")

    # Save enriched record
    with open("unified_sample_enriched.json", "w") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)
    print(f"\nSaved enriched record to unified_sample_enriched.json")