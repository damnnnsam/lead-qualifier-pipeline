#!/usr/bin/env python3
"""Generate the next batch of domains for the classification pipeline.

Includes full StoreLeads metadata so the pipeline can use platform, revenue,
product_count, country, etc. for smarter pre-classification.
"""

import gzip
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

DUMP_PATH = "storeleads_dump.sql.gz"
BATCH_SIZE = 20_000
BATCH_NUMBER = int(sys.argv[1]) if len(sys.argv) > 1 else 4
OUTPUT_FILE = f"domains_batch{BATCH_NUMBER}_20k.json"

PREVIOUS_BATCH_FILES = sorted(
    p for p in Path(".").glob("domains_*_20k.json")
    if p.name != OUTPUT_FILE
)

COLUMNS = [
    "domain", "merchant_name", "platform", "plan",
    "estimated_sales", "estimated_sales_yearly",
    "rank", "product_count", "categories",
    "city", "state_province", "country_code",
    "description", "tags", "employee_count", "scraped_at",
]


def normalize_domain(d):
    if not d:
        return ""
    d = d.lower().strip().rstrip("/")
    if d.startswith("http"):
        try:
            d = urlparse(d).netloc
        except Exception:
            pass
    if d.startswith("www."):
        d = d[4:]
    return d


def parse_int(val):
    if not val or val == "\\N":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


already_fed = set()
found_files = 0
for fname in PREVIOUS_BATCH_FILES:
    try:
        with open(fname) as f:
            for d in json.load(f):
                already_fed.add(normalize_domain(d["domain"] if isinstance(d, dict) else d))
        found_files += 1
    except FileNotFoundError:
        print(f"  (skipping {fname} — not found)")

if found_files == 0 and BATCH_NUMBER > 1:
    skip_count = (BATCH_NUMBER - 1) * BATCH_SIZE
    print(f"No previous batch files found — will skip first {skip_count:,} by rank instead")
else:
    skip_count = 0
    print(f"Already fed: {len(already_fed):,} domains across {found_files} batch files")

candidates = []
in_copy = False

with gzip.open(DUMP_PATH, "rt", encoding="utf-8") as f:
    for line in f:
        if line.startswith("COPY public.domains"):
            in_copy = True
            continue
        if in_copy:
            if line.strip() == "\\.":
                break
            parts = line.rstrip("\n").split("\t")
            if len(parts) < len(COLUMNS):
                continue

            raw_domain = parts[0]
            domain_norm = normalize_domain(raw_domain)

            if domain_norm in already_fed:
                continue

            rank = parse_int(parts[6]) or 999_999
            est_monthly = parse_int(parts[4])
            est_yearly = parse_int(parts[5])

            monthly_rev = None
            if est_monthly:
                monthly_rev = est_monthly // 100
            elif est_yearly:
                monthly_rev = est_yearly // 100 // 12

            yearly_rev = None
            if est_yearly:
                yearly_rev = est_yearly // 100
            elif est_monthly:
                yearly_rev = (est_monthly // 100) * 12

            def clean(val):
                return val if val != "\\N" else ""

            candidates.append({
                "domain": raw_domain,
                "merchant_name": clean(parts[1]),
                "platform": clean(parts[2]),
                "plan": clean(parts[3]),
                "estimated_sales_monthly": monthly_rev,
                "estimated_sales_yearly": yearly_rev,
                "rank": rank,
                "product_count": parse_int(parts[7]),
                "categories": clean(parts[8]),
                "city": clean(parts[9]),
                "state_province": clean(parts[10]),
                "country_code": clean(parts[11]),
                "description": clean(parts[12]),
                "tags": clean(parts[13]),
                "employee_count": parse_int(parts[14]),
            })

print(f"Candidates (not yet fed): {len(candidates):,}")

candidates.sort(key=lambda x: x["rank"])
if skip_count > 0:
    candidates = candidates[skip_count:]
batch = candidates[:BATCH_SIZE]

with open(OUTPUT_FILE, "w") as f:
    json.dump(batch, f)

revs = [c["estimated_sales_yearly"] or 0 for c in batch]
platforms = {}
for c in batch:
    p = c["platform"] or "unknown"
    platforms[p] = platforms.get(p, 0) + 1
countries = {}
for c in batch:
    cc = c["country_code"] or "unknown"
    countries[cc] = countries.get(cc, 0) + 1

print(f"\nBatch {BATCH_NUMBER} stats:")
print(f"  Count: {len(batch):,}")
print(f"  Rank range: {batch[0]['rank']} - {batch[-1]['rank']}")
print(f"  Avg yearly revenue: ${sum(revs) / len(revs):,.0f}")
print(f"  Median yearly revenue: ${sorted(revs)[len(revs)//2]:,.0f}")
print(f"  Revenue > $5M: {sum(1 for r in revs if r >= 5_000_000):,}")
print(f"  Revenue > $1M: {sum(1 for r in revs if r >= 1_000_000):,}")
print(f"\n  Top platforms:")
for p, n in sorted(platforms.items(), key=lambda x: -x[1])[:10]:
    print(f"    {p}: {n:,}")
print(f"\n  Top countries:")
for cc, n in sorted(countries.items(), key=lambda x: -x[1])[:10]:
    print(f"    {cc}: {n:,}")
print(f"\nSaved to {OUTPUT_FILE}")
print(f"First domain: {batch[0]['domain']} (rank {batch[0]['rank']})")
print(f"Last domain: {batch[-1]['domain']} (rank {batch[-1]['rank']})")
