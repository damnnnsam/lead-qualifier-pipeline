#!/usr/bin/env python3
"""
A/B Pipeline Comparison Test

Compares old pipeline results (with auto-classification) against new pipeline
(all domains go through LLM) to measure classification accuracy and Amazon match quality.

Usage:
  1. Select sample:    python compare_ab.py select
  2. Run new pipeline: python compare_ab.py run
  3. Generate report:  python compare_ab.py report
  4. All at once:       python compare_ab.py all
"""

import csv
import json
import os
import random
import re
import sys
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

STORELEADS_DIR = Path(os.environ.get("STORELEADS_DIR", "."))
BATCH4_INPUT = STORELEADS_DIR / os.environ.get("AB_BATCH_INPUT", "domains_batch4_20k.json")
BATCH4_RESULTS = STORELEADS_DIR / os.environ.get("AB_BATCH_RESULTS", "results_batch4.jsonl")

OUTPUT_DIR = Path(__file__).parent / "ab_test"
SAMPLE_FILE = OUTPUT_DIR / "comparison_sample.json"
NEW_RESULTS_FILE = OUTPUT_DIR / "comparison_new_results.jsonl"
REPORT_FILE = OUTPUT_DIR / "comparison_report.json"
DISAGREEMENTS_FILE = OUTPUT_DIR / "comparison_disagreements.csv"

API_URL = os.environ.get("API_URL", "https://lead-qualifier-pipeline.fly.dev")
API_KEY = os.environ.get("API_KEY", "")

SAMPLE_SIZE = 1000
AUTO_SAMPLE = 500
LLM_SAMPLE = 500
BATCH_SIZE = 250
MAX_CONCURRENT = 4
POLL_INTERVAL = 5
POLL_TIMEOUT = 600


def norm_domain(d: str) -> str:
    d = d.lower().strip()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    return d.split("/")[0]


def select_sample():
    """Stratified sample: 500 auto-classified + 500 LLM-classified from batch 4."""
    print("Loading batch 4 input domains...")
    with open(BATCH4_INPUT) as f:
        all_input = json.load(f)
    input_by_domain = {norm_domain(d["domain"]): d for d in all_input}

    print("Loading batch 4 old results...")
    old_results = {}
    with open(BATCH4_RESULTS) as f:
        for line in f:
            r = json.loads(line)
            old_results[norm_domain(r["domain"])] = r

    auto_classified = []
    llm_classified = []

    for domain, result in old_results.items():
        if domain not in input_by_domain:
            continue
        desc = result.get("classification_description", "")
        if desc.startswith("StoreLeads:"):
            auto_classified.append(domain)
        elif "job_id" not in result or len(result) > 4:
            llm_classified.append(domain)

    print(f"  Auto-classified pool: {len(auto_classified)}")
    print(f"  LLM-classified pool: {len(llm_classified)}")

    random.seed(42)
    auto_sample = random.sample(auto_classified, min(AUTO_SAMPLE, len(auto_classified)))
    llm_sample = random.sample(llm_classified, min(LLM_SAMPLE, len(llm_classified)))

    sample_domains = set(auto_sample + llm_sample)

    sample = []
    for domain in sample_domains:
        entry = input_by_domain[domain].copy()
        entry["_old_result"] = old_results[domain]
        entry["_sample_group"] = "auto" if domain in auto_sample else "llm"
        sample.append(entry)

    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(SAMPLE_FILE, "w") as f:
        json.dump(sample, f, indent=2)

    auto_count = sum(1 for s in sample if s["_sample_group"] == "auto")
    llm_count = sum(1 for s in sample if s["_sample_group"] == "llm")
    print(f"\nSample saved: {len(sample)} domains ({auto_count} auto + {llm_count} llm)")
    print(f"  -> {SAMPLE_FILE}")
    return sample


def run_new_pipeline():
    """Send sample domains through the new pipeline API."""
    with open(SAMPLE_FILE) as f:
        sample = json.load(f)

    api_domains = [entry["domain"] for entry in sample]

    already_done = set()
    if NEW_RESULTS_FILE.exists():
        with open(NEW_RESULTS_FILE) as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    already_done.add(norm_domain(r["domain"]))
        print(f"Resuming: {len(already_done)} already processed")

    remaining = [d for d in api_domains if norm_domain(d) not in already_done]
    if not remaining:
        print("All domains already processed!")
        return

    print(f"Sending {len(remaining)} domains to {API_URL} in batches of {BATCH_SIZE}...")

    session = requests.Session()
    if API_KEY:
        session.headers["x-api-key"] = API_KEY

    batches = [remaining[i:i + BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]
    write_lock = threading.Lock()

    def process_batch(batch, batch_num):
        for attempt in range(10):
            try:
                resp = session.post(f"{API_URL}/run", json={"domains": batch}, timeout=60)
                if resp.status_code == 409:
                    print(f"  Batch {batch_num}: server busy, waiting 10s...")
                    time.sleep(10)
                    continue
                resp.raise_for_status()
                job_id = resp.json()["job_id"]
                break
            except requests.RequestException as e:
                print(f"  Batch {batch_num}: submit error {e}, retrying in 5s...")
                time.sleep(5)
        else:
            print(f"  Batch {batch_num}: FAILED to submit after 10 attempts")
            return 0

        start = time.time()
        while time.time() - start < POLL_TIMEOUT:
            try:
                resp = session.get(f"{API_URL}/status/{job_id}", timeout=30)
                data = resp.json()
                if data.get("status") == "complete":
                    results = data.get("results", [])

                    with write_lock:
                        with open(NEW_RESULTS_FILE, "a") as f:
                            for r in results:
                                f.write(json.dumps(r) + "\n")

                    summary = data.get("summary", {})
                    print(f"  Batch {batch_num}: done | ecom={summary.get('ecommerce', '?')} | on_amazon={summary.get('on_amazon', '?')} | total={len(results)}")
                    return len(results)

                if data.get("status") in ("error", "failed"):
                    print(f"  Batch {batch_num}: FAILED: {data.get('error', 'unknown')}")
                    return 0
            except requests.RequestException:
                pass
            time.sleep(POLL_INTERVAL)

        print(f"  Batch {batch_num}: TIMED OUT")
        return 0

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        futures = {executor.submit(process_batch, b, i + 1): i for i, b in enumerate(batches)}
        for future in as_completed(futures):
            future.result()

    result_count = 0
    if NEW_RESULTS_FILE.exists():
        with open(NEW_RESULTS_FILE) as f:
            result_count = sum(1 for line in f if line.strip())
    print(f"\nDone. {result_count} results saved to {NEW_RESULTS_FILE}")


def generate_report():
    """Compare old vs new results and generate report + disagreements CSV."""
    with open(SAMPLE_FILE) as f:
        sample = json.load(f)

    old_by_domain = {}
    sample_group = {}
    for entry in sample:
        domain = norm_domain(entry["domain"])
        old_by_domain[domain] = entry["_old_result"]
        sample_group[domain] = entry["_sample_group"]

    new_by_domain = {}
    with open(NEW_RESULTS_FILE) as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                new_by_domain[norm_domain(r["domain"])] = r

    all_domains = set(old_by_domain.keys()) & set(new_by_domain.keys())
    print(f"Comparing {len(all_domains)} domains (old vs new)")

    classification_changes = []
    amazon_changes = []
    stats = {
        "total_compared": len(all_domains),
        "classification_same": 0,
        "classification_different": 0,
        "amazon_same": 0,
        "amazon_gained": 0,
        "amazon_lost": 0,
        "amazon_both_none": 0,
        "by_group": {"auto": {"same": 0, "different": 0}, "llm": {"same": 0, "different": 0}},
        "old_type_distribution": Counter(),
        "new_type_distribution": Counter(),
        "flip_matrix": Counter(),
    }

    for domain in sorted(all_domains):
        old = old_by_domain[domain]
        new = new_by_domain[domain]
        group = sample_group.get(domain, "unknown")

        old_type = old.get("business_type", "unknown")
        new_type = new.get("business_type", "unknown")
        stats["old_type_distribution"][old_type] += 1
        stats["new_type_distribution"][new_type] += 1

        if old_type == new_type:
            stats["classification_same"] += 1
            stats["by_group"][group]["same"] += 1
        else:
            stats["classification_different"] += 1
            stats["by_group"][group]["different"] += 1
            stats["flip_matrix"][f"{old_type} -> {new_type}"] += 1
            classification_changes.append({
                "domain": domain,
                "group": group,
                "old_type": old_type,
                "new_type": new_type,
                "old_description": old.get("classification_description", ""),
                "new_description": new.get("classification_description", ""),
                "old_category": old.get("classification_category", ""),
                "new_category": new.get("classification_category", ""),
                "old_amazon_status": old.get("amazon_status", ""),
                "new_amazon_status": new.get("amazon_status", ""),
                "old_amazon_url": old.get("amazon_store_url", ""),
                "new_amazon_url": new.get("amazon_store_url", ""),
                "old_amazon_confidence": old.get("amazon_confidence", ""),
                "new_amazon_confidence": new.get("amazon_confidence", ""),
            })

        old_amazon = old.get("amazon_status", "")
        new_amazon = new.get("amazon_status", "")
        if old_amazon == new_amazon:
            if old_amazon in ("", None):
                stats["amazon_both_none"] += 1
            else:
                stats["amazon_same"] += 1
        elif new_amazon == "on_amazon" and old_amazon != "on_amazon":
            stats["amazon_gained"] += 1
            amazon_changes.append({"domain": domain, "direction": "gained", "old": old_amazon, "new": new_amazon,
                                    "new_url": new.get("amazon_store_url", ""), "new_seller": new.get("amazon_seller_name", ""),
                                    "new_confidence": new.get("amazon_confidence", "")})
        elif old_amazon == "on_amazon" and new_amazon != "on_amazon":
            stats["amazon_lost"] += 1
            amazon_changes.append({"domain": domain, "direction": "lost", "old": old_amazon, "new": new_amazon,
                                    "old_url": old.get("amazon_store_url", ""), "old_seller": old.get("amazon_seller_name", "")})

    stats["old_type_distribution"] = dict(stats["old_type_distribution"].most_common())
    stats["new_type_distribution"] = dict(stats["new_type_distribution"].most_common())
    stats["flip_matrix"] = dict(stats["flip_matrix"].most_common())

    report = {
        "summary": stats,
        "classification_changes_count": len(classification_changes),
        "amazon_changes_count": len(amazon_changes),
        "top_flips": dict(Counter(stats["flip_matrix"]).most_common(10)),
        "amazon_gained": [c for c in amazon_changes if c["direction"] == "gained"],
        "amazon_lost": [c for c in amazon_changes if c["direction"] == "lost"],
    }

    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Report saved: {REPORT_FILE}")

    if classification_changes:
        fieldnames = list(classification_changes[0].keys())
        with open(DISAGREEMENTS_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(sorted(classification_changes, key=lambda x: x["group"]))
        print(f"Disagreements CSV: {DISAGREEMENTS_FILE} ({len(classification_changes)} rows)")

    print(f"\n{'='*60}")
    print(f"RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"Total compared: {stats['total_compared']}")
    print(f"\nClassification:")
    print(f"  Same:      {stats['classification_same']} ({stats['classification_same']/stats['total_compared']*100:.1f}%)")
    print(f"  Different: {stats['classification_different']} ({stats['classification_different']/stats['total_compared']*100:.1f}%)")
    print(f"\n  By group:")
    for g in ("auto", "llm"):
        gs = stats["by_group"][g]
        total = gs["same"] + gs["different"]
        if total > 0:
            print(f"    {g}: {gs['different']}/{total} changed ({gs['different']/total*100:.1f}%)")
    print(f"\n  Top classification flips:")
    for flip, count in list(stats["flip_matrix"].items())[:10]:
        print(f"    {flip}: {count}")
    print(f"\n  Old type distribution: {stats['old_type_distribution']}")
    print(f"  New type distribution: {stats['new_type_distribution']}")
    print(f"\nAmazon matching:")
    print(f"  Same:   {stats['amazon_same']}")
    print(f"  Gained: {stats['amazon_gained']} (new pipeline found match, old didn't)")
    print(f"  Lost:   {stats['amazon_lost']} (old had match, new didn't)")


def main():
    if len(sys.argv) < 2:
        print("Usage: python compare_ab.py [select|run|report|all]")
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "select":
        select_sample()
    elif cmd == "run":
        if not SAMPLE_FILE.exists():
            print("No sample file found. Run 'select' first.")
            sys.exit(1)
        run_new_pipeline()
    elif cmd == "report":
        if not NEW_RESULTS_FILE.exists():
            print("No new results file found. Run 'run' first.")
            sys.exit(1)
        generate_report()
    elif cmd == "all":
        select_sample()
        run_new_pipeline()
        generate_report()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
