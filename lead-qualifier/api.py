#!/usr/bin/env python3
"""
Lead Qualification Pipeline API

POST /run accepts either:
  - {"domains": ["a.com", "b.com"]}  (legacy — bare domain strings)
  - {"domains": [{"domain": "a.com", "merchant_name": "...", ...}]}  (enriched with StoreLeads)

Poll GET /status/{job_id} for results.
"""

import os
import re
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, List, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

from pipeline import run_pipeline_from_api

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_KEY = os.getenv("API_KEY", "").strip()
REQUIRE_API_KEY = bool(API_KEY)
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "16"))
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", "1200"))
MAX_DOMAINS_PER_REQUEST = int(os.getenv("MAX_DOMAINS_PER_REQUEST", "5000"))
JOB_RETENTION_SECONDS = 3600  # prune completed jobs after 1 hour

jobs: dict = {}
jobs_lock = threading.Lock()

RESULT_FIELDS = [
    "domain", "title", "business_type", "amazon_status",
    "classification_category", "classification_description",
    "amazon_store_url", "amazon_seller_name", "amazon_confidence",
    "amazon_confidence_reason", "amazon_checked_at",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def require_api_key(x_api_key: Optional[str] = Header(None)):
    if not REQUIRE_API_KEY:
        return
    if not x_api_key or x_api_key.strip() != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


def _norm(d: str) -> str:
    d = d.lower().strip()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    return d.split("/")[0]


def normalize_input(raw: List[Any]) -> List[dict]:
    """Accept list of strings or dicts. Returns deduplicated list of dicts."""
    seen = set()
    out = []
    for item in raw:
        if isinstance(item, str):
            d = _norm(item)
            if d and "." in d and len(d) > 3 and d not in seen:
                seen.add(d)
                out.append({"domain": d})
        elif isinstance(item, dict):
            d = _norm(item.get("domain", ""))
            if d and "." in d and len(d) > 3 and d not in seen:
                seen.add(d)
                item["domain"] = d
                out.append(item)
    return out


def _prune_old_jobs():
    """Remove completed/errored jobs older than JOB_RETENTION_SECONDS. Must hold jobs_lock."""
    now = time.time()
    stale = [
        jid for jid, j in jobs.items()
        if j.get("status") in ("complete", "error")
        and (now - j["created_at"]) > JOB_RETENTION_SECONDS
    ]
    for jid in stale:
        del jobs[jid]


def _set_progress(job_id: str, msg: str):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["progress"] = msg


def run_pipeline_for_domains(domain_metas: List[dict], job_id: str):
    """Run the full pipeline. Updates jobs[job_id] in-place."""
    log_path = Path(tempfile.gettempdir()) / f"pipeline_{job_id}.log"
    try:
        _set_progress(job_id, f"Starting pipeline for {len(domain_metas)} domains...")

        def progress_cb(msg: str):
            _set_progress(job_id, msg)

        final, summary = run_pipeline_from_api(domain_metas, log_path, progress_cb=progress_cb)

        results = []
        for row in final:
            results.append({k: row.get(k) for k in RESULT_FIELDS})

        with jobs_lock:
            jobs[job_id]["status"] = "complete"
            jobs[job_id]["progress"] = None
            jobs[job_id]["results"] = results
            jobs[job_id]["summary"] = summary
    except Exception as e:
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["progress"] = None
            jobs[job_id]["error"] = str(e)[:500]
    finally:
        if log_path.exists():
            try:
                log_path.unlink()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Lead Qualification Pipeline API",
    description="Submit domains (with optional StoreLeads metadata); get ecommerce + Amazon presence results.",
)


class RunRequest(BaseModel):
    domains: List[Any]


class RunResponse(BaseModel):
    job_id: str
    domain_count: int
    status: str


@app.post("/run", response_model=RunResponse)
async def run(request: RunRequest, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)

    if len(request.domains) > MAX_DOMAINS_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Too many domains ({len(request.domains)}). Max is {MAX_DOMAINS_PER_REQUEST}.",
        )

    domain_metas = normalize_input(request.domains)
    if not domain_metas:
        raise HTTPException(status_code=400, detail="No valid domains provided")

    with jobs_lock:
        _prune_old_jobs()
        now = time.time()
        for jid, j in jobs.items():
            if j.get("status") == "running" and (now - j["created_at"]) > JOB_TIMEOUT_SECONDS:
                j["status"] = "error"
                j["progress"] = None
                j["error"] = f"Timed out after {JOB_TIMEOUT_SECONDS}s"

        running = [j for j in jobs.values() if j.get("status") == "running"]
    if len(running) >= MAX_CONCURRENT_JOBS:
        raise HTTPException(
            status_code=409,
            detail=f"{len(running)}/{MAX_CONCURRENT_JOBS} jobs running. Wait for one to finish.",
        )

    job_id = str(uuid.uuid4())[:12]
    with jobs_lock:
        jobs[job_id] = {
            "status": "running",
            "progress": "Starting...",
            "results": None,
            "summary": None,
            "error": None,
            "created_at": time.time(),
        }

    thread = threading.Thread(
        target=run_pipeline_for_domains,
        args=(domain_metas, job_id),
        daemon=True,
    )
    thread.start()

    return RunResponse(job_id=job_id, domain_count=len(domain_metas), status="running")


@app.get("/status/{job_id}")
async def status(job_id: str, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    out = {
        "job_id": job_id,
        "status": job["status"],
        "created_at": job["created_at"],
    }
    if job.get("progress"):
        out["progress"] = job["progress"]
    if job.get("summary"):
        out["summary"] = job["summary"]
    if job.get("results") is not None:
        out["results"] = job["results"]
    if job.get("error"):
        out["error"] = job["error"]
    return out


@app.get("/jobs")
async def list_jobs(x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    with jobs_lock:
        list_ = [
            {"job_id": jid, "status": data["status"], "created_at": data["created_at"]}
            for jid, data in jobs.items()
        ]
    list_.sort(key=lambda x: -x["created_at"])
    return {"jobs": list_}


@app.get("/")
async def root():
    return {"service": "Lead Qualification Pipeline API", "docs": "/docs"}
