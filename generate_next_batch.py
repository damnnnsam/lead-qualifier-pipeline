#!/usr/bin/env python3
"""Generate a batch of domains for the classification pipeline.

Usage:
  python generate_next_batch.py                    # all remaining domains
  python generate_next_batch.py --size 50000       # 50K domains
  python generate_next_batch.py --size 20000 --batch-num 7  # old-style numbered batch
  python generate_next_batch.py --fresh             # ignore previous batches, export all

Includes full StoreLeads metadata so the pipeline can use platform, revenue,
product_count, country, etc. for smarter pre-classification.
"""

import argparse
import gzip
import json
from pathlib import Path
from urllib.parse import urlparse

DUMP_PATH = "storeleads_dump.sql.gz"

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


def load_already_fed(exclude_file: str) -> set[str]:
    already = set()
    found = 0
    for fname in sorted(Path(".").glob("domains_*.json")):
        if fname.name == exclude_file:
            continue
        try:
            with open(fname) as f:
                for d in json.load(f):
                    already.add(normalize_domain(d["domain"] if isinstance(d, dict) else d))
            found += 1
        except FileNotFoundError:
            pass
    if found:
        print(f"Already fed: {len(already):,} domains across {found} batch files")
    return already


def parse_dump(already_fed: set[str]) -> list[dict]:
    candidates = []
    in_copy = False

    def clean(val):
        return val if val != "\\N" else ""

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

    return candidates


def print_stats(batch: list[dict], label: str, output_file: str):
    revs = [c["estimated_sales_yearly"] or 0 for c in batch]
    platforms = {}
    for c in batch:
        p = c["platform"] or "unknown"
        platforms[p] = platforms.get(p, 0) + 1
    countries = {}
    for c in batch:
        cc = c["country_code"] or "unknown"
        countries[cc] = countries.get(cc, 0) + 1

    print(f"\n{label} stats:")
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
    print(f"\nSaved to {output_file}")
    print(f"First domain: {batch[0]['domain']} (rank {batch[0]['rank']})")
    print(f"Last domain: {batch[-1]['domain']} (rank {batch[-1]['rank']})")


def main():
    parser = argparse.ArgumentParser(description="Generate domain batch for classification pipeline")
    parser.add_argument("--size", type=int, default=0, help="Batch size (0 = all remaining)")
    parser.add_argument("--batch-num", type=int, default=None, help="Batch number (for numbered output files)")
    parser.add_argument("--output", type=str, default=None, help="Output filename (overrides auto-naming)")
    parser.add_argument("--fresh", action="store_true", help="Ignore previous batch files, export everything")

    # Backward compat: bare positional arg treated as batch number
    parser.add_argument("legacy_batch_num", nargs="?", type=int, default=None)
    args = parser.parse_args()

    batch_num = args.batch_num or args.legacy_batch_num

    if args.output:
        output_file = args.output
    elif batch_num:
        size_label = f"{args.size // 1000}k" if args.size else "all"
        output_file = f"domains_batch{batch_num}_{size_label}.json"
    else:
        output_file = "domains_all.json"

    if args.fresh:
        already_fed = set()
        print("Fresh mode: ignoring all previous batch files")
    else:
        already_fed = load_already_fed(output_file)

    candidates = parse_dump(already_fed)
    print(f"Candidates available: {len(candidates):,}")

    candidates.sort(key=lambda x: x["rank"])

    if args.size > 0:
        batch = candidates[:args.size]
    else:
        batch = candidates

    if not batch:
        print("No candidates to export.")
        return

    with open(output_file, "w") as f:
        json.dump(batch, f)

    label = f"Batch {batch_num}" if batch_num else "Batch"
    print_stats(batch, label, output_file)


if __name__ == "__main__":
    main()
