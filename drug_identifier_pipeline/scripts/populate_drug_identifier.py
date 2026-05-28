"""
Populate drugdb.drug_identifier from DrugMasterLinkage.combined_clean_jsonb.

Uses a server-side streaming JOIN cursor so Python never holds more than
one batch in memory regardless of how large the dataset is.

Key JSON path correction vs. original spec:
  openfda_metadata is nested at openfda.openfda_metadata (not top-level).
  Strength matching uses dailymed.drug_info.products[].active_ingredients[0].strength
  (not imprint) since active_ingredients is consistently populated.
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from itertools import groupby

import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_dir, dry_run):
    os.makedirs(log_dir, exist_ok=True)
    suffix = "_dryrun" if dry_run else ""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"drug_identifier_populate{suffix}_{ts}.log")

    logger = logging.getLogger("drug_identifier")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Logging to: {log_path}")
    return logger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _as_dict(val):
    """Ensure JSONB value is a Python dict (psycopg2 may return str or dict)."""
    if val is None:
        return {}
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}
    return val


def parse_strength_mg(text):
    """Extract a numeric strength in MG from strings like '100 mg' or '12.5 MG'."""
    if not text:
        return None
    m = re.search(r'([\d.]+)\s*mg', str(text), re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_identifiers(jsonb, fid_by_rxcui, all_fids, logger):
    """
    Returns list of (formulation_id_str, id_type, id_value) tuples.

    jsonb          : combined_clean_jsonb as a Python dict
    fid_by_rxcui   : { rxcui_str: formulation_id_str } built from streamed drug rows
    all_fids       : [ formulation_id_str, ... ] for this drug
    """
    rows = []
    if not jsonb or not all_fids:
        return rows

    openfda      = _as_dict(jsonb.get("openfda", {}))
    openfda_meta = _as_dict(openfda.get("openfda_metadata", {}))
    rxnorm_arr   = jsonb.get("rxnorm", []) or []
    dailymed     = _as_dict(jsonb.get("dailymed", {}))
    dm_products  = (_as_dict(dailymed.get("drug_info", {})).get("products", []) or [])
    drugbank_arr = jsonb.get("drugbank", []) or []

    # Build strength → generic rxcui map from rxnorm array
    # Only entries with ingredients are generic formulations
    strength_to_rxcui = {}  # { float_mass: rxcui_str }
    for entry in rxnorm_arr:
        ings = entry.get("ingredients") or []
        if ings:
            mass = (ings[0].get("scdc") or {}).get("mass")
            if mass is not None:
                try:
                    strength_to_rxcui[float(mass)] = str(entry["rxcui"])
                except (ValueError, KeyError):
                    pass

    def _fid_for_rxcui(rxcui_val):
        """Map a rxcui (possibly brand) to a formulation_id."""
        rxcui_str = str(rxcui_val)
        fid = fid_by_rxcui.get(rxcui_str)
        if fid:
            return fid
        # Brand rxcui — find matching generic via strength in formulation name
        for entry in rxnorm_arr:
            if str(entry.get("rxcui", "")) == rxcui_str:
                name = entry.get("brand_formulation") or entry.get("generic_formulation", "")
                strength = parse_strength_mg(name)
                if strength is not None:
                    gen_rxcui = strength_to_rxcui.get(strength)
                    if gen_rxcui:
                        return fid_by_rxcui.get(gen_rxcui)
        return None

    def _fid_for_strength(strength_mg):
        """Map a strength in MG to formulation_id via rxnorm → drug table."""
        gen_rxcui = strength_to_rxcui.get(float(strength_mg)) if strength_mg is not None else None
        if gen_rxcui:
            return fid_by_rxcui.get(gen_rxcui)
        return None

    # -------------------------------------------------------------------
    # 1. rxcui
    # -------------------------------------------------------------------
    for rxcui_val in (openfda_meta.get("rxcui") or []):
        fid = _fid_for_rxcui(rxcui_val)
        if fid is None and len(all_fids) == 1:
            fid = all_fids[0]
        if fid:
            rows.append((fid, "rxcui", str(rxcui_val)))

    # -------------------------------------------------------------------
    # 2. ndc_product + application_number (per dailymed product)
    #    Build ndc_to_fid map needed for ndc_package and upc.
    # -------------------------------------------------------------------
    ndc_to_fid = {}      # { ndc_code: formulation_id_str }
    seen_product_ndcs = set()

    for product in dm_products:
        ndc_code    = product.get("ndc_code")
        approval_id = product.get("approval_id")

        fid = None

        # Prefer active_ingredients strength for matching
        active_ings = product.get("active_ingredients") or []
        if active_ings:
            strength_str = (active_ings[0] or {}).get("strength", "")
            strength = parse_strength_mg(strength_str)
            fid = _fid_for_strength(strength)

        # Fallback: imprint — try standard "250 mg" format first,
        # then context-aware token scan against known strengths (handles "V;625" style)
        if fid is None:
            imprint = (product.get("physical_characteristics") or {}).get("imprint", "")
            fid = _fid_for_strength(parse_strength_mg(imprint))
            if fid is None and imprint:
                for token in re.split(r'[;,\s]+', str(imprint)):
                    try:
                        val = float(token)
                        if val in strength_to_rxcui:
                            fid = fid_by_rxcui.get(strength_to_rxcui[val])
                            if fid:
                                break
                    except ValueError:
                        pass

        # Fallback: single formulation
        if fid is None and len(all_fids) == 1:
            fid = all_fids[0]

        if fid:
            if ndc_code:
                ndc_to_fid[ndc_code] = fid
                seen_product_ndcs.add(ndc_code)
                rows.append((fid, "ndc_product", ndc_code))
            if approval_id:
                rows.append((fid, "application_number", approval_id))
        elif len(all_fids) == 1:
            # Single formulation — safe to assign even without a strength match
            fid = all_fids[0]
            if ndc_code:
                ndc_to_fid[ndc_code] = fid
                seen_product_ndcs.add(ndc_code)
                rows.append((fid, "ndc_product", ndc_code))
            if approval_id:
                rows.append((fid, "application_number", approval_id))
        else:
            # Multiple formulations, no strength match — skip to avoid cross-mapping.
            # NDC/application_number are formulation-specific; wrong mapping is worse than none.
            if ndc_code:
                seen_product_ndcs.add(ndc_code)
            logger.debug(
                f"Skipping ndc_product/app_num for ndc={ndc_code}: "
                f"no strength match among {len(all_fids)} formulations"
            )

    # Pick up any product_ndc from openfda_metadata not already covered by dailymed
    for pndc in (openfda_meta.get("product_ndc") or []):
        if pndc not in seen_product_ndcs:
            seen_product_ndcs.add(pndc)
            fid = ndc_to_fid.get(pndc)
            if fid is None and len(all_fids) == 1:
                fid = all_fids[0]
            if fid:  # only insert if we have a confident match — no cross-map fan-out
                rows.append((fid, "ndc_product", pndc))

    # -------------------------------------------------------------------
    # 3. ndc_package — prefix-match to ndc_to_fid (skip if no confident match)
    # -------------------------------------------------------------------
    for pkg_ndc in (openfda_meta.get("package_ndc") or []):
        parts = str(pkg_ndc).split("-")
        if len(parts) >= 2:
            product_prefix = "-".join(parts[:2])
            fid = ndc_to_fid.get(product_prefix)
            if fid:
                rows.append((fid, "ndc_package", pkg_ndc))
            elif len(all_fids) == 1:
                rows.append((all_fids[0], "ndc_package", pkg_ndc))

    # -------------------------------------------------------------------
    # 4. unii → ALL formulations
    # -------------------------------------------------------------------
    for unii in (openfda_meta.get("unii") or []):
        for f in all_fids:
            rows.append((f, "unii", str(unii)))

    # -------------------------------------------------------------------
    # 5. upc → try NDC digit-suffix match, else all formulations
    # -------------------------------------------------------------------
    for upc in (openfda_meta.get("upc") or []):
        upc_str = str(upc)
        matched_fid = None
        for ndc, fid in ndc_to_fid.items():
            ndc_digits = ndc.replace("-", "")
            if ndc_digits in upc_str:
                matched_fid = fid
                break
        if matched_fid:
            rows.append((matched_fid, "upc", upc_str))
        else:
            for f in all_fids:
                rows.append((f, "upc", upc_str))

    # -------------------------------------------------------------------
    # 6. spl_id → ALL formulations
    # -------------------------------------------------------------------
    for spl_id in (openfda_meta.get("spl_id") or []):
        for f in all_fids:
            rows.append((f, "spl_id", str(spl_id)))

    # -------------------------------------------------------------------
    # 7. spl_set_id → ALL formulations
    # -------------------------------------------------------------------
    for spl_set_id in (openfda_meta.get("spl_set_id") or []):
        for f in all_fids:
            rows.append((f, "spl_set_id", str(spl_set_id)))

    # -------------------------------------------------------------------
    # 8. drugbank — pick active drug entry, apply to all formulations
    # -------------------------------------------------------------------
    generic_name_hint = (
        (_as_dict(openfda.get("drug_info", {})).get("generic_name", "") or "")
        .lower().strip()
    )
    active_db_id = None
    for db_entry in drugbank_arr:
        db_entry = _as_dict(db_entry) if not isinstance(db_entry, dict) else db_entry
        drug_info = _as_dict(db_entry.get("drug_info", {}))
        clinical  = _as_dict(db_entry.get("clinical", {}))
        db_id     = drug_info.get("drugbank_id", "")
        db_name   = (drug_info.get("name", "") or "").lower().strip()
        indication = clinical.get("indication", "") or ""

        if not db_id:
            continue

        is_active = False
        if indication and len(indication) > 20:
            is_active = True
        elif generic_name_hint and db_name:
            if (generic_name_hint in db_name or db_name in generic_name_hint
                    or (len(generic_name_hint) >= 5 and generic_name_hint[:5] == db_name[:5])):
                is_active = True

        if is_active:
            active_db_id = db_id
            break

    if active_db_id:
        for f in all_fids:
            rows.append((f, "drugbank", active_db_id))

    return rows


# ---------------------------------------------------------------------------
# Batch insert
# ---------------------------------------------------------------------------

def flush_batch(cursor, batch, dry_run):
    if not batch:
        return 0
    unique_batch = list(set(batch))
    if dry_run:
        return len(unique_batch)
    cursor.executemany(
        """
        INSERT INTO drugdb.drug_identifier (formulation_id, id_type, id_value)
        VALUES (%s, %s, %s)
        ON CONFLICT (formulation_id, id_type, id_value) DO NOTHING
        """,
        unique_batch,
    )
    return len(unique_batch)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Populate drug_identifier table")
    parser.add_argument("--password",   required=True,              help="PostgreSQL password")
    parser.add_argument("--host",       default=os.environ.get("DB_HOST", "localhost"))
    parser.add_argument("--port",       type=int, default=5432)
    parser.add_argument("--dbname",     default="postgres")
    parser.add_argument("--user",       default="postgres")
    parser.add_argument("--limit",      type=int, default=None,     help="Limit DrugMasterLinkage records (testing)")
    parser.add_argument("--dry-run",    action="store_true",         help="Extract but do not insert")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--log-dir",    default="logs/")
    args = parser.parse_args()

    logger = setup_logging(args.log_dir, args.dry_run)
    logger.info(
        f"Starting | dry_run={args.dry_run} | limit={args.limit} | batch_size={args.batch_size}"
    )

    db_kwargs = dict(host=args.host, port=args.port, dbname=args.dbname,
                     user=args.user, password=args.password)

    # Two separate connections so write commits don't kill the read cursor.
    read_conn = psycopg2.connect(**db_kwargs)
    read_conn.autocommit = False         # named cursor needs a transaction; we never commit it
    psycopg2.extras.register_default_jsonb(read_conn)

    write_conn = psycopg2.connect(**db_kwargs)
    write_conn.autocommit = False
    write_cur = write_conn.cursor()

    # Build the streaming query — LIMIT applies to DrugMasterLinkage rows (not JOIN rows)
    if args.limit:
        stream_sql = f"""
            SELECT
                dml.master_linkage_id,
                dml.combined_clean_jsonb,
                d.formulation_id,
                d.rxcui
            FROM (
                SELECT master_linkage_id, combined_clean_jsonb
                FROM public."DrugMasterLinkage"
                WHERE combined_clean_jsonb IS NOT NULL
                ORDER BY master_linkage_id
                LIMIT {args.limit}
            ) dml
            JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
            ORDER BY dml.master_linkage_id
        """
    else:
        stream_sql = """
            SELECT
                dml.master_linkage_id,
                dml.combined_clean_jsonb,
                d.formulation_id,
                d.rxcui
            FROM public."DrugMasterLinkage" dml
            JOIN drugdb.drug d ON d.master_linkage_id = dml.master_linkage_id
            WHERE dml.combined_clean_jsonb IS NOT NULL
            ORDER BY dml.master_linkage_id
        """

    stream_cur = read_conn.cursor(name="drug_identifier_stream")
    stream_cur.itersize = 500
    stream_cur.execute(stream_sql)

    batch          = []
    drugs_done     = 0
    rows_extracted = 0
    rows_inserted  = 0
    errors         = 0

    for master_id, group in groupby(stream_cur, key=lambda r: r[0]):
        group_list = list(group)
        # jsonb is the same for all rows in this group (same DrugMasterLinkage record)
        raw_jsonb = group_list[0][1]
        jsonb = _as_dict(raw_jsonb)

        fid_by_rxcui = {}
        all_fids = []
        for _, _, fid, rxcui in group_list:
            fid_str = str(fid)
            all_fids.append(fid_str)
            if rxcui:
                fid_by_rxcui[str(rxcui)] = fid_str

        try:
            new_rows = extract_identifiers(jsonb, fid_by_rxcui, all_fids, logger)
            rows_extracted += len(new_rows)
            batch.extend(new_rows)
        except Exception as exc:
            logger.error(f"Error master_linkage_id={master_id}: {exc}", exc_info=True)
            errors += 1

        drugs_done += 1

        if len(batch) >= args.batch_size:
            inserted = flush_batch(write_cur, batch, args.dry_run)
            if not args.dry_run:
                write_conn.commit()
            rows_inserted += inserted
            batch = []
            logger.info(
                f"Progress: {drugs_done} drugs | {rows_inserted} rows inserted | {errors} errors"
            )

    # Final flush
    if batch:
        inserted = flush_batch(write_cur, batch, args.dry_run)
        if not args.dry_run:
            write_conn.commit()
        rows_inserted += inserted

    try:
        stream_cur.close()
    except Exception:
        pass
    write_cur.close()
    read_conn.close()
    write_conn.close()

    summary = (
        f"\n╔══════════════════════════════════════════════╗\n"
        f"║         drug_identifier COMPLETE             ║\n"
        f"╠══════════════════════════════════════════════╣\n"
        f"║  dry_run          : {str(args.dry_run):<25}║\n"
        f"║  Drugs processed  : {drugs_done:<25}║\n"
        f"║  Rows extracted   : {rows_extracted:<25}║\n"
        f"║  Rows inserted    : {rows_inserted:<25}║\n"
        f"║  Errors           : {errors:<25}║\n"
        f"╚══════════════════════════════════════════════╝"
    )
    logger.info(summary)


if __name__ == "__main__":
    main()
