# scripts/export/

## Purpose

These scripts export data out of the database to local files. They are read-only from a data-integrity perspective (no schema changes, no updates) and are used to produce snapshots of database content for auditing, offline inspection, or handoff. Run them whenever you need a file-based copy of a DB table's contents.

## Scripts

### export_durg_master_linkage.py
- **What it does:** Streams all rows from `DrugMasterLinkage.combined_clean_jsonb` to a pretty-printed text file using a server-side cursor (memory-efficient, `itersize` = batch size). Each record is separated by a `--- Record N ---` header, with `null` written for NULL JSONB values. Prints progress every 5,000 records and a final summary (row count, null count, elapsed time, file size).
- **Reads from:** `DrugMasterLinkage.combined_clean_jsonb` (PostgreSQL — all rows)
- **Writes to:** `<output_dir>/durg_master_linkage_<YYYYMMDD_HHMMSS>.txt` (timestamped file in the directory specified by `--output-dir`, default: current working directory)
- **When to run:** On-demand, whenever a file snapshot of `DrugMasterLinkage` is needed. The export is non-destructive and safe to run at any time.
- **Usage:**
  ```bash
  python scripts/export/export_durg_master_linkage.py \
      --host 178.236.185.230 \
      --dbname postgres \
      --user postgres \
      --password YOUR_PASSWORD \
      --output-dir /path/to/output \
      --batch-size 1000
  ```

  All arguments except `--dbname` and `--user` have defaults. Password can also be supplied via the `PGPASSWORD` environment variable.

## Dependencies

**Python packages:** `psycopg2-binary`

**Environment variable (optional):** `PGPASSWORD` — used if `--password` is not provided on the command line.

**Database tables that must exist:**
- `DrugMasterLinkage` with column `combined_clean_jsonb` (JSONB)

**Disk space:** The output file can be very large. `DrugMasterLinkage` with ~50k records of deeply nested JSONB will typically produce multi-GB output files. Ensure the output directory has sufficient free space before running.
