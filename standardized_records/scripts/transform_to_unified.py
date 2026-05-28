"""
transform_to_unified.py
-----------------------
Consumes the raw consolidated JSON (openFDA + DailyMed + RxNorm + DrugBank)
and emits a single unified record matching cdss_unified_schema.json.

This is the ingest-time transformer. It:
  1. Merges overlapping narrative fields across openFDA and DailyMed
     (DailyMed wins on structure, openFDA wins on completeness).
  2. Normalises RxNorm entries into generic/brand clinical formulations.
  3. Folds DrugBank interactions onto active ingredients.
  4. Preserves label tables verbatim, tagged with semantic_type.
  5. Emits structured_facts stubs that a downstream NLP/LLM pass fills in.

Structured extraction (interactions severity, dosing regimen population filters,
MedDRA coding, etc.) is intentionally left as a second pass, because it needs
either regex+rule extraction or an LLM call per section. The transformer
populates everything that's deterministic; stubs carry the raw text so the
second pass has something to work from.
"""

import hashlib
import json
from typing import Any


# ---------- small utils ----------

def _as_list(x: Any) -> list:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _nz(x: Any) -> bool:
    return x not in (None, "", [], {})


def _span(source: str, doc_id: str, section_code: str, excerpt: str = "") -> dict:
    return {
        "source": source,
        "document_id": doc_id,
        "section_code": section_code,
        "excerpt": excerpt[:400] if excerpt else ""
    }


def _formulation_id(openfda: dict, dailymed: dict) -> str:
    sid = openfda.get("set_id") or dailymed.get("drug_label", {}).get("document_id")
    if sid:
        return sid
    # fallback
    seed = json.dumps(openfda.get("drug_info", {}), sort_keys=True) + \
           json.dumps(dailymed.get("products", []), sort_keys=True)
    return "fml_" + hashlib.sha1(seed.encode()).hexdigest()[:16]


# ---------- section-by-section builders ----------

def build_identifiers(raw: dict) -> dict:
    openfda = raw.get("openfda", {}) or {}
    meta = openfda.get("openfda_metadata", {}) or {}
    dailymed = raw.get("dailymed", {}) or {}
    drugbank = raw.get("drugbank", []) or []

    product_ndcs = list(meta.get("product_ndc", []))
    package_ndcs = list(meta.get("package_ndc", []))
    # DailyMed products contribute NDCs too
    for p in dailymed.get("products", []) or []:
        if p.get("ndc_code") and p["ndc_code"] not in product_ndcs:
            product_ndcs.append(p["ndc_code"])

    return {
        "spl_set_id":          openfda.get("set_id") or meta.get("spl_set_id", [None])[0],
        "spl_document_id":     openfda.get("document_id") or dailymed.get("drug_label", {}).get("document_id"),
        "ndc_product":         product_ndcs,
        "ndc_package":         package_ndcs,
        "upc":                 list(meta.get("upc", [])),
        "rxcui":               list(meta.get("rxcui", [])),
        "unii":                list(meta.get("unii", [])),
        "drugbank_ids":        [db.get("drugbank_id") for db in drugbank if db.get("drugbank_id")],
        "application_numbers": list(meta.get("application_number", [])),
    }


def build_drug(raw: dict) -> dict:
    openfda = raw.get("openfda", {}) or {}
    meta = openfda.get("openfda_metadata", {}) or {}
    dailymed = raw.get("dailymed", {}) or {}
    drugbank = raw.get("drugbank", []) or []
    info = openfda.get("drug_info", {}) or {}

    # Map substance -> unii/drugbank
    substance_to_unii = {}
    for s, u in zip(meta.get("substance_name", []), meta.get("unii", [])):
        substance_to_unii[s.upper()] = u
    name_to_drugbank = {db["name"].upper(): db["drugbank_id"]
                        for db in drugbank if db.get("name") and db.get("drugbank_id")}

    # Gather active ingredients: prefer DailyMed products, else generic_name
    actives = []
    seen = set()
    for p in dailymed.get("products", []) or []:
        for ing in p.get("active_ingredients", []) or []:
            nm = (ing if isinstance(ing, str) else ing.get("name", "")).upper()
            if nm and nm not in seen:
                actives.append({
                    "substance_name": nm,
                    "unii":           substance_to_unii.get(nm),
                    "drugbank_id":    name_to_drugbank.get(nm),
                    "strength":       None, "strength_value": None, "strength_unit": None,
                })
                seen.add(nm)
    if not actives and info.get("generic_name"):
        nm = info["generic_name"].upper()
        actives.append({
            "substance_name": nm,
            "unii":           substance_to_unii.get(nm),
            "drugbank_id":    name_to_drugbank.get(nm),
            "strength":       None, "strength_value": None, "strength_unit": None,
        })

    # Inactive ingredients: union from all products
    inactives, seen_i = [], set()
    for p in dailymed.get("products", []) or []:
        for ing in p.get("inactive_ingredients", []) or []:
            nm = (ing if isinstance(ing, str) else ing.get("name", "")).upper()
            if nm and nm not in seen_i and nm not in seen:  # exclude actives mistakenly listed
                inactives.append({
                    "name":        nm,
                    "unii":        substance_to_unii.get(nm),
                    "drugbank_id": name_to_drugbank.get(nm),
                    "role":        None
                })
                seen_i.add(nm)

    return {
        "generic_name": info.get("generic_name") or (actives[0]["substance_name"] if actives else None),
        "brand_names":  [info["brand_name"]] if info.get("brand_name") else list(meta.get("brand_name", [])),
        "product_type": info.get("product_type"),
        "active_ingredients":   actives,
        "inactive_ingredients": inactives,
        "drug_class":           [],   # filled by NLP pass
        "atc_codes":            [],   # filled by external lookup
        "mechanism_of_action":  openfda.get("mechanism_of_action")
    }


def build_product(raw: dict) -> dict:
    openfda = raw.get("openfda", {}) or {}
    dailymed = raw.get("dailymed", {}) or {}
    info = openfda.get("drug_info", {}) or {}
    meta = openfda.get("openfda_metadata", {}) or {}

    skus = []
    forms = set()
    routes = set(_as_list(info.get("route"))) | set(meta.get("route", []) or [])

    for p in dailymed.get("products", []) or []:
        forms.add(p.get("dosage_form"))
        if p.get("route_of_administration"):
            routes.add(p["route_of_administration"])
        skus.append({
            "ndc_product":    p.get("ndc_code"),
            "ndc_package":    None,
            "upc":            None,
            "package_type":   (p.get("packaging") or {}).get("type"),
            "package_qty":    (p.get("packaging") or {}).get("quantity"),
            "strength_label": None,
            "physical":       p.get("physical_characteristics") or {}
        })

    return {
        "manufacturer":  info.get("manufacturer") or (dailymed.get("manufacturer") or {}).get("name"),
        "routes":        [r for r in routes if r],
        "dosage_forms":  [f for f in forms if f],
        "skus":          skus,
        "storage":       openfda.get("storage_and_handling"),
        "how_supplied":  openfda.get("how_supplied_storage")
    }


def build_rxnorm(raw: dict) -> dict:
    out = []
    for r in raw.get("rxnorm", []) or []:
        is_brand = _nz(r.get("brand_formulation"))
        name = r.get("brand_formulation") if is_brand else r.get("generic_formulation")
        out.append({
            "rxcui":     r.get("rxcui"),
            "tty":       None,  # not provided in raw; lookup in RxNorm API if needed
            "name":      name,
            "kind":      "brand" if is_brand else "generic",
            "dose_form": r.get("specific_dosage_form"),
            "synonyms":  [s.strip().strip("'\"") for s in
                          (r.get("synonyms") or "").strip("[]").split(",") if s.strip()]
        })
    return {"clinical_formulations": out}


# ---------- clinical narratives (merged openFDA + DailyMed) ----------

# map: unified field -> (openfda_key, dailymed_label_section_key)
_SECTION_MAP = {
    "indications_and_usage":       ("indications_and_usage",       "indications_and_usage"),
    "dosage_and_administration":   ("dosage_and_administration",   "dosage_and_administration"),
    "contraindications":           ("contraindications",           "contraindications"),
    "warnings_and_precautions":    ("warnings_and_cautions",       "warnings_and_precautions"),
    "adverse_reactions":           ("adverse_reactions",           "adverse_reactions"),
    "drug_interactions":           ("drug_interactions",           "drug_interactions"),
    "use_in_specific_populations": ("use_in_specific_populations", None),
    "pediatric_use":               ("pediatric_use",               None),
    "geriatric_use":               ("geriatric_use",               None),
    "use_in_pregnancy":            ("use_in_pregnancy",            None),
    "clinical_pharmacology":       ("clinical_pharmacology",       "clinical_pharmacology"),
    "pharmacokinetics":            ("pharmacokinetics",            "pharmacokinetics"),
    "pharmacodynamics":            ("pharmacodynamics",            None),
    "mechanism_of_action":         ("mechanism_of_action",         None),
    "microbiology":                ("microbiology",                None),
    "overdosage":                  ("overdosage",                  None),
    "nonclinical_toxicology":      ("nonclinical_toxicology",      None),
    "clinical_studies":            ("clinical_studies",            "clinical_studies"),
    "information_for_patients":    ("information_for_patients",    "information_for_patients"),
    "spl_patient_package_insert":  ("spl_patient_package_insert",  None),
}


def _flatten_subsections(section_dict: dict, prefix: str) -> list:
    """Recursively flatten DailyMed nested subsections into a flat list."""
    out = []
    if not isinstance(section_dict, dict):
        return out
    subs = section_dict.get("subsections") or []
    for i, s in enumerate(subs, 1):
        sub_id = f"{prefix}.{i}"
        out.append({
            "subsection_id": sub_id,
            "title":         (s.get("section_title") or "").strip(),
            "text":          s.get("content", "") or ""
        })
        out.extend(_flatten_subsections(s, sub_id))
    return out


def build_clinical(raw: dict) -> dict:
    openfda = raw.get("openfda", {}) or {}
    dm_sections = (raw.get("dailymed", {}) or {}).get("label_sections", {}) or {}
    openfda_docid = openfda.get("document_id", "")
    dm_docid = (raw.get("dailymed", {}) or {}).get("drug_label", {}).get("document_id", "")

    out = {}
    for unified_key, (ofda_key, dm_key) in _SECTION_MAP.items():
        ofda_text = openfda.get(ofda_key) if ofda_key else None
        dm_section = dm_sections.get(dm_key) if dm_key else None

        if not ofda_text and not dm_section:
            continue

        # prefer openFDA text (it's usually the full narrative); use DailyMed for subsection structure
        text = ofda_text or (dm_section or {}).get("content", "")
        subsections = _flatten_subsections(dm_section, unified_key) if dm_section else []

        src = "merged" if (ofda_text and dm_section) else ("openfda" if ofda_text else "dailymed")
        doc_id = openfda_docid if ofda_text else dm_docid

        out[unified_key] = {
            "text":        text,
            "subsections": subsections,
            "source":      src,
            "source_span": _span(src if src != "merged" else "openfda", doc_id, unified_key, (text or "")[:200])
        }
    return out


# ---------- structured tables ----------

_TABLE_KEY_TO_SEMANTIC = {
    "dosage_tables":                  "dosing",
    "drug_interactions_table":        "interaction",
    "contraindications_table":        "contraindication",
    "adverse_reactions_tables":       "adverse_event",
    "pharmacokinetics_table":         "pharmacokinetics",
    "clinical_pharmacology_table":    "pharmacokinetics",
    "clinical_studies_table":         "clinical_study",
    "spl_patient_package_insert_table": "other"
}


def build_label_tables(raw: dict) -> list:
    """Dedup tables across sibling keys (pharmacokinetics_table and
    clinical_pharmacology_table share the same rows in SPL output)."""
    openfda = raw.get("openfda", {}) or {}
    out, i, seen = [], 0, set()
    for key, stype in _TABLE_KEY_TO_SEMANTIC.items():
        for t in openfda.get(key, []) or []:
            sig = (t.get("caption", ""), json.dumps(t.get("rows", []), sort_keys=True))
            if sig in seen:
                continue
            seen.add(sig)
            i += 1
            out.append({
                "table_id":      f"tbl_{i:03d}",
                "caption":       t.get("caption", ""),
                "semantic_type": stype,
                "section":       key,
                "headers":       t.get("headers", []) or [],
                "rows":          t.get("rows", []) or []
            })
    return out


# ---------- structured_facts seeds ----------

def build_structured_facts(raw: dict) -> dict:
    """
    Seeds structured_facts with the parts we can build deterministically:
      - interactions from DrugBank (authoritative, structured)
      - contraindications from openFDA contraindications_table
      - dosing_regimens from openFDA dosage_tables (rows preserved verbatim)
    Free-text extraction (severity classification, MedDRA coding,
    population parsing, indication -> ICD-10 mapping) is done in a second pass.
    """
    openfda = raw.get("openfda", {}) or {}
    drugbank = raw.get("drugbank", []) or []
    doc_id = openfda.get("document_id", "")

    interactions = []
    for db in drugbank:
        db_name = db.get("name") or ""
        # classify whether this drugbank entry is an active or inactive ingredient,
        # so the CDSS can weight "an excipient interacts with drug X" differently
        # from "the active ingredient interacts with drug X".
        substance_role = "unknown"
        active_names = {
            (ai.get("substance_name") or "").upper()
            for ai in (raw.get("openfda", {}).get("openfda_metadata", {}) or {}).get("substance_name", [])
        } if False else set()
        # rebuild from the unified drug if already built - fall back to openFDA
        info = (raw.get("openfda", {}) or {}).get("drug_info", {}) or {}
        if info.get("generic_name") and db_name.upper() in info["generic_name"].upper():
            substance_role = "active_ingredient"
        else:
            dm_products = (raw.get("dailymed", {}) or {}).get("products", []) or []
            inactive_union = set()
            for p in dm_products:
                for ing in p.get("inactive_ingredients", []) or []:
                    inactive_union.add((ing if isinstance(ing, str) else ing.get("name","")).upper())
            if db_name.upper() in inactive_union:
                substance_role = "excipient"

        for di in db.get("drug_interactions", []) or []:
            interactions.append({
                "interaction_id": f"db_{db.get('drugbank_id','')}_{di.get('drugbank_id','')}",
                "subject_substance":      db_name,
                "subject_drugbank_id":    db.get("drugbank_id"),
                "subject_substance_role": substance_role,
                "partner": {
                    "kind":        "drug",
                    "name":        di.get("name"),
                    "rxcui":       None,
                    "drugbank_id": di.get("drugbank_id"),
                    "atc":         None
                },
                "effect_direction":    None,
                "effect_on":           None,
                "magnitude":           None,
                "mechanism":           None,
                "severity":            "unknown",
                "clinical_management": di.get("description"),
                "evidence_level":      "established",
                "source":              "drugbank",
                "source_span": {
                    "source":       "drugbank",
                    "document_id":  db.get("drugbank_id"),
                    "section_code": "drug_interactions",
                    "excerpt":      (di.get("description") or "")[:400]
                }
            })

    contraindications = []
    for t in openfda.get("contraindications_table", []) or []:
        for row in t.get("rows", []) or []:
            if not isinstance(row, dict):
                continue
            drugs = row.get("Drugs Within Class That Are Contraindicated With VIRACEPT") \
                    or row.get("Drugs") or ""
            reason = row.get("Clinical Comment") or ""
            dclass = row.get("Drug Class") or ""
            contraindications.append({
                "kind":        "coadministered_drug",
                "term":        drugs,
                "rxcui":       None,
                "drugbank_id": None,
                "drug_class":  dclass,
                "reason":      reason,
                "severity":    "absolute",
                "source_span": _span("openfda", doc_id, "contraindications_table", reason[:200])
            })

    # Dosing regimens: seed from dosage_tables, real parsing in second pass
    dosing_regimens = []
    for idx, t in enumerate(openfda.get("dosage_tables", []) or [], 1):
        dosing_regimens.append({
            "regimen_id":    f"dose_tbl_{idx}",
            "indication":    None,
            "population":    {},   # filled by second pass
            "route":         None,
            "dose_amount":   None,
            "dose_value":    None,
            "dose_unit":     None,
            "dose_basis":    None,
            "frequency":     None,
            "duration":      None,
            "max_daily_dose":None,
            "administration_notes": t.get("caption", ""),
            "adjustment_required_for": [],
            "source_span":   _span("openfda", doc_id, f"dosage_tables[{idx-1}]", t.get("caption",""))
        })

    return {
        "indications":        [],
        "contraindications":  contraindications,
        "interactions":       interactions,
        "dosing_regimens":    dosing_regimens,
        "adverse_events":     [],
        "warnings":           [],
        "pharmacology": {
            "targets":         [],
            "metabolism":      [],
            "transporters":    [],
            "half_life":       None,
            "bioavailability": None,
            "protein_binding": None,
            "excretion":       None
        }
    }


# ---------- top-level ----------

def transform(raw: dict) -> dict:
    openfda = raw.get("openfda", {}) or {}
    dailymed = raw.get("dailymed", {}) or {}

    record = {
        "formulation_id":   _formulation_id(openfda, dailymed),
        "record_version":   "1.0",
        "last_ingested_at": None,   # set at ingest time
        "identifiers":      build_identifiers(raw),
        "drug":             build_drug(raw),
        "product":          build_product(raw),
        "rxnorm":           build_rxnorm(raw),
        "clinical":         build_clinical(raw),
        "structured_facts": build_structured_facts(raw),
        "label_tables":     build_label_tables(raw),
        "provenance": {
            "openfda":  {"set_id": openfda.get("set_id"),
                         "version": openfda.get("version"),
                         "effective_date": openfda.get("effective_date")},
            "dailymed": {"document_id": (dailymed.get("drug_label") or {}).get("document_id"),
                         "effective_date": (dailymed.get("drug_label") or {}).get("effective_date")},
            "rxnorm":   {"fetched_at": None},
            "drugbank": {"version": None, "fetched_at": None}
        },
        "sources": {
            "has_openfda":  bool(openfda),
            "has_dailymed": bool(dailymed),
            "has_rxnorm":   bool(raw.get("rxnorm")),
            "has_drugbank": bool(raw.get("drugbank"))
        }
    }
    return record


if __name__ == "__main__":
    import sys
    src = sys.argv[1] if len(sys.argv) > 1 else \
          "/mnt/user-data/uploads/ConsolidatedJSonFromFDSDrugBankDailyMedRXnorm.json"
    dst = sys.argv[2] if len(sys.argv) > 2 else "unified_record.json"
    with open(src) as f:
        raw = json.load(f)
    unified = transform(raw)
    with open(dst, "w") as f:
        json.dump(unified, f, indent=2, ensure_ascii=False)
    print(f"wrote {dst}")