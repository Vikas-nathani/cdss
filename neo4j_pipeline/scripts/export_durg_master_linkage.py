#!/usr/bin/env python3
"""
Export durgMasterLinkage.combined_clean_jsonb to a pretty-printed text file.
"""

import json
import os
import sys
import argparse
from datetime import datetime

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="Export durgMasterLinkage to text file")
    parser.add_argument("--host", default="localhost", help="DB host (default: localhost)")
    parser.add_argument("--port", default=5432, type=int, help="DB port (default: 5432)")
    parser.add_argument("--dbname", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", default=None, help="Database password (or set PGPASSWORD env var)")
    parser.add_argument("--output-dir", default=".", help="Output directory (default: current dir)")
    parser.add_argument("--batch-size", default=1000, type=int, help="Records per batch (default: 1000)")
    return parser.parse_args()


def connect(args):
    password = args.password or os.environ.get("PGPASSWORD")
    print(f"Connecting to {args.user}@{args.host}:{args.port}/{args.dbname} ...")
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=password,
    )
    conn.autocommit = False
    print("  Connected.\n")
    return conn


def get_total_count(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM \"DrugMasterLinkage\"")
        return cur.fetchone()[0]


def export(conn, output_path, batch_size=1000):
    total = get_total_count(conn)
    print(f"Total records to export: {total:,}\n")

    null_count = 0
    exported = 0
    progress_interval = 5000

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# Export: durgMasterLinkage.combined_clean_jsonb\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write(f"# Total records: {total:,}\n")
        f.write("#" * 60 + "\n\n")

        # Server-side cursor for memory-efficient streaming
        with conn.cursor("export_cursor", cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.itersize = batch_size
            cur.execute('SELECT combined_clean_jsonb FROM "DrugMasterLinkage"')

            for row in cur:
                exported += 1
                record_num = exported
                value = row[0]

                f.write(f"--- Record {record_num} ---\n")

                if value is None:
                    f.write("null\n")
                    null_count += 1
                else:
                    # psycopg2 returns JSONB as a Python dict/list already
                    if isinstance(value, (dict, list)):
                        f.write(json.dumps(value, indent=2, ensure_ascii=False))
                    else:
                        # Fallback: parse string
                        try:
                            f.write(json.dumps(json.loads(value), indent=2, ensure_ascii=False))
                        except (json.JSONDecodeError, TypeError):
                            f.write(str(value))

                f.write("\n\n")

                if exported % progress_interval == 0:
                    pct = exported / total * 100
                    print(f"  Progress: {exported:,} / {total:,} ({pct:.1f}%)")

    return exported, null_count


def main():
    args = parse_args()

    conn = None
    try:
        conn = connect(args)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"durg_master_linkage_{timestamp}.txt"
        output_path = os.path.join(args.output_dir, filename)

        print(f"Output file: {output_path}\n")

        start = datetime.now()
        exported, null_count = export(conn, output_path, batch_size=args.batch_size)
        elapsed = (datetime.now() - start).total_seconds()

        file_size_bytes = os.path.getsize(output_path)
        file_size_mb = file_size_bytes / (1024 * 1024)

        print(f"\n{'='*50}")
        print(f"Export complete.")
        print(f"  Records exported : {exported:,}")
        print(f"  Null values      : {null_count:,}")
        print(f"  Elapsed time     : {elapsed:.1f}s")
        print(f"  Output file      : {output_path}")
        print(f"  File size        : {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
        print(f"{'='*50}")

    except psycopg2.OperationalError as e:
        print(f"\nERROR: Could not connect to database.\n{e}")
        sys.exit(1)
    except psycopg2.Error as e:
        print(f"\nERROR: Database error.\n{e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(1)
    finally:
        if conn and not conn.closed:
            conn.close()
            print("Connection closed.")


if __name__ == "__main__":
    main()
