#!/usr/bin/env python3
"""Upload pipeline results (JSONL or CSV) to BigQuery ecom_brain.api_results.

Usage:
  python upload_results_to_bq.py results_batch6.jsonl
  python upload_results_to_bq.py results_batch6.csv
  python upload_results_to_bq.py results_batch6.jsonl results_batch5.jsonl
  python upload_results_to_bq.py --dry-run results_batch6.jsonl

Reads BQ credentials from .env (same service account used by the orchestrator).
Deduplicates by domain within each upload. Skips rows already in BQ unless --replace.
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

PROJECT = os.getenv("BIGQUERY_PROJECT", "deliverability-486315")
DATASET = os.getenv("BIGQUERY_DATASET", "ecom_brain")
TABLE = f"{PROJECT}.{DATASET}.api_results"

SCHEMA = [
    {"name": "domain", "type": "STRING", "mode": "REQUIRED"},
    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
    {"name": "business_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amazon_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "classification_category", "type": "STRING", "mode": "NULLABLE"},
    {"name": "classification_description", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amazon_store_url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amazon_seller_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amazon_confidence", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "amazon_confidence_reason", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amazon_checked_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "job_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
]

FIELD_NAMES = {f["name"] for f in SCHEMA}


def _norm(d: str) -> str:
    d = d.lower().strip()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    return d.split("/")[0]


def build_credentials():
    from google.oauth2 import service_account

    info = {
        "type": os.getenv("GOOGLE_SERVICE_ACCOUNT_TYPE"),
        "project_id": os.getenv("GOOGLE_SERVICE_ACCOUNT_PROJECT_ID"),
        "private_key_id": os.getenv("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY_ID"),
        "private_key": os.getenv("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY"),
        "client_email": os.getenv("GOOGLE_SERVICE_ACCOUNT_CLIENT_EMAIL"),
        "client_id": os.getenv("GOOGLE_SERVICE_ACCOUNT_CLIENT_ID"),
        "auth_uri": os.getenv("GOOGLE_SERVICE_ACCOUNT_AUTH_URI"),
        "token_uri": os.getenv("GOOGLE_SERVICE_ACCOUNT_TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("GOOGLE_SERVICE_ACCOUNT_AUTH_PROVIDER_CERT_URL"),
        "client_x509_cert_url": os.getenv("GOOGLE_SERVICE_ACCOUNT_CLIENT_CERT_URL"),
        "universe_domain": os.getenv("GOOGLE_SERVICE_ACCOUNT_UNIVERSE_DOMAIN"),
    }
    missing = [k for k, v in info.items() if not v]
    if missing:
        sys.exit(f"Missing service account env vars: {', '.join(missing)}")
    return service_account.Credentials.from_service_account_info(info)


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def load_csv_file(path: Path) -> list[dict]:
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def normalize_row(raw: dict) -> dict:
    domain = _norm(raw.get("domain", ""))
    if not domain:
        return None

    row = {"domain": domain}

    for field in SCHEMA:
        name = field["name"]
        if name == "domain":
            continue
        val = raw.get(name)
        if val is None or val == "":
            row[name] = None
            continue
        if field["type"] == "FLOAT":
            try:
                row[name] = float(val)
            except (ValueError, TypeError):
                row[name] = None
        elif field["type"] == "TIMESTAMP":
            row[name] = val
        else:
            row[name] = str(val)

    if not row.get("created_at"):
        row["created_at"] = datetime.now(timezone.utc).isoformat()

    if not row.get("job_id"):
        row["job_id"] = raw.get("job_id")

    return row


def load_existing_domains(client) -> set[str]:
    query = f"SELECT REGEXP_REPLACE(LOWER(TRIM(CAST(domain AS STRING))), r'^www\\\\.', '') AS d FROM `{TABLE}`"
    try:
        result = client.query(query).result()
        return {row.d for row in result if row.d}
    except Exception as e:
        print(f"  Could not read existing domains (table may not exist yet): {e}")
        return set()


def main():
    parser = argparse.ArgumentParser(description="Upload pipeline results to BigQuery ecom_brain.api_results")
    parser.add_argument("files", nargs="+", help="JSONL or CSV result files to upload")
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate but don't upload")
    parser.add_argument("--replace", action="store_true", help="Re-upload domains even if they exist in BQ")
    args = parser.parse_args()

    all_rows = []
    for filepath in args.files:
        p = Path(filepath)
        if not p.exists():
            print(f"File not found: {p}")
            continue
        if p.suffix == ".csv":
            raw = load_csv_file(p)
        else:
            raw = load_jsonl(p)
        print(f"Loaded {len(raw):,} rows from {p.name}")
        all_rows.extend(raw)

    seen = set()
    normalized = []
    for raw in all_rows:
        row = normalize_row(raw)
        if not row:
            continue
        if row["domain"] in seen:
            continue
        seen.add(row["domain"])
        normalized.append(row)

    print(f"Normalized: {len(normalized):,} unique domains")

    btype_counts = {}
    for r in normalized:
        bt = r.get("business_type") or "unknown"
        btype_counts[bt] = btype_counts.get(bt, 0) + 1
    print("Business types:")
    for bt, c in sorted(btype_counts.items(), key=lambda x: -x[1]):
        print(f"  {bt}: {c:,}")

    if args.dry_run:
        print("\n--dry-run: skipping upload")
        return

    from google.cloud import bigquery

    credentials = build_credentials()
    client = bigquery.Client(project=PROJECT, credentials=credentials)

    if not args.replace:
        existing = load_existing_domains(client)
        before = len(normalized)
        normalized = [r for r in normalized if r["domain"] not in existing]
        skipped = before - len(normalized)
        if skipped:
            print(f"Skipped {skipped:,} domains already in BQ")
    
    if not normalized:
        print("Nothing new to upload.")
        return

    print(f"\nUploading {len(normalized):,} rows to {TABLE}...")

    schema_bq = [
        bigquery.SchemaField(f["name"], f["type"], mode=f["mode"])
        for f in SCHEMA
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema_bq,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    import io
    ndjson = "\n".join(json.dumps(r) for r in normalized)
    job = client.load_table_from_file(
        io.BytesIO(ndjson.encode("utf-8")),
        TABLE,
        job_config=job_config,
    )
    job.result()

    print(f"Uploaded {job.output_rows:,} rows to {TABLE}")
    print("Done.")


if __name__ == "__main__":
    main()
