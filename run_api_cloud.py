"""
API client — sends enriched StoreLeads domain objects to the pipeline API.

Now sends full metadata (platform, categories, revenue, merchant_name, etc.)
so the pipeline can pre-classify and use merchant_name for Amazon matching.
"""

import json
import os
import re
import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

API_URL = os.environ.get("API_URL", "https://lead-qualifier-pipeline.fly.dev")
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    sys.exit("ERROR: API_KEY environment variable is required. Set it in .env or export it.")
DOMAINS_FILE = os.environ.get("DOMAINS_FILE")
RESULTS_FILE = os.environ.get("RESULTS_FILE")
if not DOMAINS_FILE or not RESULTS_FILE:
    sys.exit("ERROR: DOMAINS_FILE and RESULTS_FILE environment variables are required.")


def _norm_domain(d: str) -> str:
    d = d.lower().strip()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    return d.split("/")[0]

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "750"))
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "8"))
POLL_INTERVAL = 3
POLL_TIMEOUT = 1200
MAX_RETRIES = 60

write_lock = threading.Lock()
shutdown_requested = False


def handle_sigint(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    print("\nShutdown requested — finishing active jobs...")


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


def load_domains():
    with open(DOMAINS_FILE) as f:
        return json.load(f)


def load_completed():
    completed = set()
    if not os.path.exists(RESULTS_FILE):
        return completed
    with open(RESULTS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                completed.add(_norm_domain(r["domain"]))
            except (json.JSONDecodeError, KeyError):
                continue
    return completed


def append_results(results):
    os.makedirs(os.path.dirname(RESULTS_FILE) or ".", exist_ok=True)
    with write_lock:
        with open(RESULTS_FILE, "a") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")
            f.flush()
            os.fsync(f.fileno())


def submit_batch(session, batch):
    """Submit a batch of enriched domain objects to the API."""
    for attempt in range(MAX_RETRIES):
        if shutdown_requested:
            return None
        try:
            resp = session.post(
                f"{API_URL}/run",
                json={"domains": batch},
                timeout=60,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"  Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code == 409:
                wait = 5
                print(f"  Server busy (409) — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            wait = min(5 * (attempt + 1), 30)
            print(f"  Submit error: {e} — retry in {wait}s")
            time.sleep(wait)
    return None


def poll_job(session, job_id):
    start = time.time()
    last_progress = None
    while time.time() - start < POLL_TIMEOUT:
        if shutdown_requested:
            return None
        try:
            resp = session.get(f"{API_URL}/status/{job_id}", timeout=30)
            resp.raise_for_status()
            data = resp.json()

            progress = data.get("progress")
            if progress and progress != last_progress:
                print(f"  [{job_id}] {progress}")
                last_progress = progress

            if data.get("status") == "complete":
                return data
            if data.get("status") in ("failed", "error"):
                print(f"  [{job_id}] FAILED: {data.get('error', 'unknown')}")
                return None
        except requests.RequestException:
            pass
        time.sleep(POLL_INTERVAL)
    print(f"  [{job_id}] Timed out after {POLL_TIMEOUT}s")
    return None


def wait_for_server_clear(session):
    for _ in range(30):
        try:
            resp = session.get(f"{API_URL}/jobs", timeout=10)
            running = [j for j in resp.json().get("jobs", []) if j["status"] == "running"]
            if not running:
                return
            print(f"  Waiting for {len(running)} server job(s) to finish...")
            time.sleep(10)
        except requests.RequestException:
            time.sleep(5)


def process_batch(session, batch, batch_num, total_batches, all_count, counter):
    """Submit, poll, save one batch. Returns count saved."""
    submission = submit_batch(session, batch)
    if not submission:
        print(f"Batch {batch_num + 1}/{total_batches} — submit failed, skipping")
        return 0

    job_id = submission["job_id"]
    result = poll_job(session, job_id)
    if not result:
        return 0

    ecom_results = result.get("results", [])
    ecom_domains = {_norm_domain(r["domain"]) for r in ecom_results}

    to_save = list(ecom_results)
    for d in batch:
        domain = d["domain"] if isinstance(d, dict) else d
        if _norm_domain(domain) not in ecom_domains:
            to_save.append({
                "domain": _norm_domain(domain),
                "business_type": "not_ecommerce",
                "job_id": job_id,
            })

    append_results(to_save)

    with counter["lock"]:
        counter["done"] += len(to_save)
        processed = counter["done"]

    elapsed = time.time() - counter["start"]
    rate = processed / elapsed * 3600 if elapsed > 0 else 0
    summary = result.get("summary", {})
    online = summary.get("online", "?")
    ecom = summary.get("ecommerce", summary.get("auto_ecommerce", "?"))
    on_amz = summary.get("on_amazon", "?")

    print(
        f"Batch {batch_num + 1}/{total_batches} done | "
        f"{processed:,}/{all_count:,} total | "
        f"{rate:,.0f}/hr | "
        f"online={online} ecom={ecom} on_amazon={on_amz} | "
        f"{elapsed / 60:.1f}m elapsed"
    )
    return len(to_save)


def main():
    session = requests.Session()
    session.headers["x-api-key"] = API_KEY

    print("Checking for stale server jobs...")
    wait_for_server_clear(session)
    print("Server clear.\n")

    all_domains = load_domains()
    completed = load_completed()
    remaining = [d for d in all_domains if _norm_domain(d["domain"] if isinstance(d, dict) else d) not in completed]

    print(f"Total domains:  {len(all_domains)}")
    print(f"Already done:   {len(completed)}")
    print(f"Remaining:      {len(remaining)}")
    print(f"Batch size:     {BATCH_SIZE}")
    print(f"Concurrent jobs: {MAX_CONCURRENT_JOBS}")

    if not remaining:
        print("All domains already processed!")
        return

    batches = [remaining[i:i + BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]
    total_batches = len(batches)
    counter = {"done": len(completed), "lock": threading.Lock(), "start": time.time()}

    print(f"Batches to run: {total_batches}")
    print(f"Starting...\n")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
        futures = {}
        for idx, batch in enumerate(batches):
            if shutdown_requested:
                break
            future = executor.submit(
                process_batch, session, batch, idx,
                total_batches, len(all_domains), counter
            )
            futures[future] = idx

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Batch error: {e}")

    elapsed = time.time() - counter["start"]
    print(f"\nFinished: {counter['done']:,} domains processed in {elapsed / 60:.1f} minutes")
    print(f"Results saved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
