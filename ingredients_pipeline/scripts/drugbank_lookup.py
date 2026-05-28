#!/usr/bin/env python3
"""
DrugBank ID lookup for 190 skeleton ingredient rows.

Strategy (in order):
  1. RxNorm REST API   — free, no auth, returns DRUGBANK codes directly
  2. PubChem REST API  — free, no auth, cross-references via drug name
  3. DrugBank website  — fallback scrape with session cookies

Output: /data/drugbank_lookup_results.csv
"""

import csv
import json
import re
import shutil
import time
import urllib.parse
from collections import Counter
from pathlib import Path

import pandas as pd
import requests

INPUT_CSV  = "/home/nathanivikas890_gmail_com/cdss/data/data-1777879031060.csv"
OUTPUT_CSV = "/home/nathanivikas890_gmail_com/cdss/data/drugbank_lookup_results.csv"
CKPT_CSV   = "/home/nathanivikas890_gmail_com/cdss/data/drugbank_lookup_checkpoint.csv"

DB_PATTERN = re.compile(r"DB\d{5}")

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────

def get(session: requests.Session, url: str, *, delay: float = 0.5,
        retries: int = 3, timeout: int = 20) -> requests.Response | None:
    for attempt in range(retries):
        try:
            time.sleep(delay)
            r = session.get(url, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 403):
                wait = 8 * (attempt + 1)
                print(f"    [{r.status_code}] rate-limited, sleeping {wait}s…")
                time.sleep(wait)
            else:
                print(f"    [{r.status_code}] {url[:80]}")
                time.sleep(3)
        except (requests.Timeout, requests.ConnectionError) as e:
            print(f"    network error attempt {attempt+1}: {e}")
            time.sleep(5)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Strategy 1 — RxNorm REST API
# ──────────────────────────────────────────────────────────────────────────────

def rxnorm_lookup(rxcui: str, session: requests.Session) -> list[str]:
    """Return list of DrugBank IDs from RxNorm allProperties endpoint."""
    url = f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allProperties.json?prop=ALL"
    r = get(session, url, delay=0.3)
    if r is None:
        return []
    try:
        data = r.json()
        concepts = data.get("propConceptGroup", {}).get("propConcept", [])
        return [c["propValue"] for c in concepts if c.get("propName") == "DRUGBANK"]
    except (json.JSONDecodeError, KeyError):
        return []


def rxnorm_name(rxcui: str, session: requests.Session) -> str:
    """Return preferred name from RxNorm."""
    url = f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/properties.json"
    r = get(session, url, delay=0.2)
    if r is None:
        return ""
    try:
        return r.json().get("properties", {}).get("name", "")
    except (json.JSONDecodeError, KeyError):
        return ""


# ──────────────────────────────────────────────────────────────────────────────
# Strategy 2 — PubChem REST API (name → DrugBank xref)
# ──────────────────────────────────────────────────────────────────────────────

def pubchem_lookup(name: str, session: requests.Session) -> list[str]:
    """Return list of DrugBank IDs from PubChem by drug name."""
    enc = urllib.parse.quote(name)
    url = (f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
           f"{enc}/xrefs/RegistryID/JSON")
    r = get(session, url, delay=0.5)
    if r is None or r.status_code == 404:
        return []
    try:
        rows = r.json().get("InformationList", {}).get("Information", [])
        ids = []
        for row in rows:
            for rid in row.get("RegistryID", []):
                if DB_PATTERN.match(rid):
                    ids.append(rid)
        return ids
    except (json.JSONDecodeError, KeyError):
        return []


def pubchem_lookup_variants(name: str, session: requests.Session) -> list[str]:
    """Try multiple name simplifications against PubChem."""
    variants = _name_variants(name)
    for v in variants:
        ids = pubchem_lookup(v, session)
        if ids:
            return ids
    return []


# ──────────────────────────────────────────────────────────────────────────────
# Strategy 3 — RxNorm approximate-term search (finds related rxcuis with DB IDs)
# ──────────────────────────────────────────────────────────────────────────────

def rxnorm_approximate(name: str, session: requests.Session) -> list[str]:
    """
    Use RxNorm approximate-term search to find closely related concepts,
    then check each candidate rxcui for a DRUGBANK property.
    Returns list of DrugBank IDs found (may be from a near-match concept).
    """
    for variant in _name_variants(name):
        enc = urllib.parse.quote(variant)
        url = f"https://rxnav.nlm.nih.gov/REST/approximateTerm.json?term={enc}&maxEntries=5"
        r = get(session, url, delay=0.3)
        if r is None:
            continue
        try:
            candidates = (r.json()
                          .get("approximateGroup", {})
                          .get("candidate", []))
        except (json.JSONDecodeError, KeyError):
            continue

        seen_rxcuis: set[str] = set()
        for c in candidates:
            cid = c.get("rxcui", "")
            if not cid or cid in seen_rxcuis:
                continue
            seen_rxcuis.add(cid)
            db_ids = rxnorm_lookup(cid, session)
            if db_ids:
                return db_ids
    return []


# ──────────────────────────────────────────────────────────────────────────────
# Name simplification helpers
# ──────────────────────────────────────────────────────────────────────────────

_SUFFIXES = [
    ", usp", " usp", "(usp)", " (usp)",
    ", human", " (human)", " (chicken)",
]
_EXTRACT_SUFFIXES = [
    " allergenic extract", " smut allergenic extract",
    " pollen extract", " extract", " allergenic", " pollen",
]

def _name_variants(name: str) -> list[str]:
    """Return a de-duplicated list of search variants, most specific first."""
    variants = [name]

    # strip USP/human suffixes
    s = name.lower()
    for suf in _SUFFIXES:
        s = s.replace(suf, "")
    s = s.strip().rstrip(",")
    if s != name.lower():
        variants.append(s)

    # strip extract suffixes
    b = s
    for suf in _EXTRACT_SUFFIXES:
        b = b.replace(suf, "")
    b = b.strip().rstrip(",")
    if b and b != s:
        variants.append(b)

    # deduplicate preserving order
    seen = set()
    out = []
    for v in variants:
        if v.lower() not in seen:
            seen.add(v.lower())
            out.append(v)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Match scoring
# ──────────────────────────────────────────────────────────────────────────────

def score_match(ingredient_name: str, drugbank_name: str) -> tuple[str, str]:
    ing = ingredient_name.lower().strip()
    db  = drugbank_name.lower().strip()
    if ing == db:
        return "GREEN", "EXACT"
    variants = _name_variants(ingredient_name)
    for v in variants:
        if v.lower() == db or db == v.lower():
            return "GREEN", "CLOSE_NAME"
    if db in ing or ing in db:
        return "GREEN", "CLOSE_NAME"
    for v in variants:
        if v.lower() and v.lower() in db:
            return "YELLOW", "PARTIAL"
        if db and db in v.lower():
            return "YELLOW", "PARTIAL"
    return "YELLOW", "PARTIAL"


# ──────────────────────────────────────────────────────────────────────────────
# Main lookup orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def lookup(rxcui: str, name: str, session: requests.Session) -> dict:

    result = {
        "rxcui": rxcui,
        "ingredient_name": name,
        "drugbank_id": "",
        "drugbank_name": "",
        "confidence": "RED",
        "match_type": "NOT_FOUND",
        "related_drugbank_ids": "",
        "search_url": f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allProperties.json?prop=ALL",
        "notes": "",
    }

    # ── 1. RxNorm exact rxcui lookup ──────────────────────────────────────────
    db_ids = rxnorm_lookup(rxcui, session)
    if db_ids:
        rn = rxnorm_name(rxcui, session)
        result.update(
            drugbank_id=db_ids[0],
            related_drugbank_ids=",".join(db_ids[1:10]),
            confidence="GREEN",
            match_type="RXNORM_XREF",
            drugbank_name=rn or name,
            notes=f"RxNorm returned {len(db_ids)} DrugBank ID(s)",
        )
        return result

    # ── 2. PubChem by name ────────────────────────────────────────────────────
    print(f"    RxNorm: no DRUGBANK code → PubChem…")
    db_ids = pubchem_lookup_variants(name, session)
    if db_ids:
        result.update(
            drugbank_id=db_ids[0],
            related_drugbank_ids=",".join(db_ids[1:10]),
            confidence="GREEN",
            match_type="PUBCHEM_XREF",
            search_url=f"https://pubchem.ncbi.nlm.nih.gov/compound/{urllib.parse.quote(name)}",
            drugbank_name=name,
            notes=f"PubChem returned {len(db_ids)} DrugBank ID(s)",
        )
        return result

    # ── 3. RxNorm approximate-term (finds related concepts with DB IDs) ───────
    print(f"    PubChem: nothing → RxNorm approximate…")
    db_ids = rxnorm_approximate(name, session)
    if db_ids:
        result.update(
            drugbank_id=db_ids[0],
            related_drugbank_ids=",".join(db_ids[1:10]),
            confidence="YELLOW",
            match_type="RXNORM_APPROX",
            search_url=(f"https://rxnav.nlm.nih.gov/REST/approximateTerm.json?"
                        f"term={urllib.parse.quote(name)}&maxEntries=5"),
            drugbank_name=name,
            notes="Matched via RxNorm approximate-term search (near-match concept)",
        )
        return result

    # ── nothing found ─────────────────────────────────────────────────────────
    result["notes"] = "Not found in RxNorm, PubChem, or RxNorm approximate search"
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Checkpoint helpers
# ──────────────────────────────────────────────────────────────────────────────

FIELDNAMES = [
    "rxcui", "ingredient_name", "drugbank_id", "drugbank_name",
    "confidence", "match_type", "related_drugbank_ids", "search_url", "notes",
]

def load_checkpoint() -> dict[str, dict]:
    done: dict[str, dict] = {}
    if Path(CKPT_CSV).exists():
        with open(CKPT_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done[row["rxcui"]] = row
        print(f"Resumed from checkpoint: {len(done)} already done.")
    return done

def save_checkpoint(results: list[dict]) -> None:
    with open(CKPT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(results)


# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────

def print_summary(results: list[dict]) -> None:
    counts = Counter(r["confidence"] for r in results)
    print("\n=== DRUGBANK LOOKUP SUMMARY ===")
    print(f"Total ingredients  : {len(results)}")
    print(f"GREEN (confirmed)  : {counts.get('GREEN', 0)}")
    print(f"YELLOW (partial)   : {counts.get('YELLOW', 0)}")
    print(f"RED (not found)    : {counts.get('RED', 0)}")

    print("\nGREEN matches:")
    for i, r in enumerate([x for x in results if x["confidence"] == "GREEN"], 1):
        print(f"  {i:3}. rxcui={r['rxcui']:>10} | {r['ingredient_name'][:50]:<50} → {r['drugbank_id']} [{r['match_type']}]")

    print("\nYELLOW (partial):")
    for i, r in enumerate([x for x in results if x["confidence"] == "YELLOW"], 1):
        print(f"  {i:3}. rxcui={r['rxcui']:>10} | {r['ingredient_name'][:50]:<50} → {r['drugbank_id']} [{r['match_type']}]")

    print("\nRED (not found):")
    for i, r in enumerate([x for x in results if x["confidence"] == "RED"], 1):
        print(f"  {i:3}. rxcui={r['rxcui']:>10} | {r['ingredient_name']}")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} ingredients from {INPUT_CSV}")

    done      = load_checkpoint()
    results   = list(done.values())
    done_keys = set(done.keys())

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    pending = df[~df["rxcui"].astype(str).isin(done_keys)]
    print(f"To process: {len(pending)}\n")

    for seq, (_, row) in enumerate(pending.iterrows(), 1):
        rxcui = str(row["rxcui"])
        name  = str(row["name"]).strip()
        print(f"[{seq}/{len(pending)}] rxcui={rxcui} | {name}")

        try:
            res = lookup(rxcui, name, session)
        except Exception as exc:
            print(f"  EXCEPTION: {exc}")
            res = {
                "rxcui": rxcui, "ingredient_name": name,
                "drugbank_id": "", "drugbank_name": "",
                "confidence": "RED", "match_type": "ERROR",
                "related_drugbank_ids": "", "search_url": "",
                "notes": f"EXCEPTION: {exc}",
            }

        results.append(res)
        tag = f"{res['confidence']} → {res['drugbank_id'] or 'NOT FOUND'}"
        if res["drugbank_name"]:
            tag += f" ({res['drugbank_name']})"
        print(f"  {tag}")

        if seq % 10 == 0:
            save_checkpoint(results)
            print(f"  [checkpoint: {len(results)} saved]")

    save_checkpoint(results)
    shutil.copy(CKPT_CSV, OUTPUT_CSV)
    print(f"\nOutput written to {OUTPUT_CSV}")
    print_summary(results)


if __name__ == "__main__":
    main()
