# Lead Qualifier Pipeline

Company enrichment pipeline that takes domains (with StoreLeads metadata) and determines:

1. Whether the business is ecommerce (pre-classification + LLM classification)
2. Whether the business sells on Amazon (Serper SERP search + LLM brand matching)

Output: qualified lead list with domain, business type, Amazon store URL, seller name, and confidence score.

## Architecture

```
Client (run_api_cloud.py)
 │  HTTP POST /run — 750 domains per batch, 8 concurrent jobs
 ▼
FastAPI API (lead-qualifier/api.py) ── deployed on Fly.io
 │
 ▼
Pipeline (lead-qualifier/pipeline.py)
 ├── Phase 0: Pre-classify using StoreLeads metadata (skip obvious non-ecom)
 ├── Phase 1: Scrape remaining domains via BrightData proxy (500 concurrent)
 ├── Phase 2: Classify with Claude LLM (enriched prompt w/ metadata)
 └── Phase 3: Amazon presence check (Serper SERP + Claude brand matching)
```

## Project Structure

```
lead-qualifier-pipeline/
├── lead-qualifier/              # Deployable unit (Fly.io)
│   ├── api.py                   # FastAPI — POST /run, GET /status/{job_id}, GET /jobs
│   ├── pipeline.py              # Core pipeline (pre-classify, scrape, classify, Amazon check)
│   ├── ai/
│   │   ├── llm_provider.py      # Claude/DeepSeek LLM client abstraction
│   │   └── brand_matcher.py     # Amazon brand matching via LLM
│   ├── serper/
│   │   └── serp_search.py       # Serper.dev SERP API wrapper
│   ├── Dockerfile
│   ├── fly.toml                 # Fly.io config (performance-8x, 32 GB, scale-to-zero)
│   ├── requirements.txt
│   └── .env.example
├── run_api_cloud.py             # Client: sends domain batches to the API
├── generate_next_batch.py       # Generates batch JSON from storeleads_dump.sql.gz
└── compare_ab.py                # A/B test: compare pipeline versions
```

## Setup

### Environment Variables

Copy the example and fill in your keys:

```bash
cp lead-qualifier/.env.example lead-qualifier/.env
```

| Variable | Required | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | Claude API access |
| `SERPER_API_KEY` | Yes | Serper.dev SERP search |
| `API_KEY` | Yes | Auth token for the pipeline API |
| `BRIGHTDATA_PROXY` | No | Proxy URL for web scraping |
| `LLM_PROVIDER` | No | `claude` (default) or `deepseek` |
| `LLM_CONCURRENCY` | No | Concurrent LLM calls (default: 500) |
| `MAX_CONCURRENT_JOBS` | No | Max parallel pipeline jobs (default: 16) |

### Local Development

```bash
cd lead-qualifier
pip install -r requirements.txt
uvicorn api:app --host 0.0.0.0 --port 8080
```

The API serves interactive docs at `http://localhost:8080/docs`.

### Deploy to Fly.io

```bash
cd lead-qualifier
fly deploy
```

Set secrets on the Fly app:

```bash
fly secrets set ANTHROPIC_API_KEY=sk-... SERPER_API_KEY=... API_KEY=...
```

The VM is `performance-8x` / 32 GB RAM and scales to zero when idle.

## Usage

### 1. Generate a batch

Requires `storeleads_dump.sql.gz` in the project root.

```bash
python generate_next_batch.py 7    # creates domains_batch7_20k.json
```

### 2. Run the batch

Set env vars and run:

```bash
export API_KEY="your-api-key"
export DOMAINS_FILE="domains_batch7_20k.json"
export RESULTS_FILE="results_batch7.jsonl"
python run_api_cloud.py
```

The client sends 750-domain batches (8 concurrent), polls for results, and appends to the JSONL results file. It's resumable — already-processed domains are skipped on restart.

### 3. Results format

Each line in the results JSONL file:

```json
{
  "domain": "example.com",
  "business_type": "ecommerce",
  "classification_category": "Pet Supplies",
  "classification_description": "Online pet supply store selling food, toys, and accessories",
  "amazon_status": "on_amazon",
  "amazon_store_url": "https://www.amazon.com/stores/Example/page/...",
  "amazon_seller_name": "Example Pet Co",
  "amazon_confidence": 4,
  "amazon_confidence_reason": "Exact brand name match on Amazon storefront"
}
```

## API Endpoints

All endpoints require `X-API-Key` header.

| Method | Path | Description |
|---|---|---|
| `POST` | `/run` | Submit domains for processing. Body: `{"domains": [...]}` |
| `GET` | `/status/{job_id}` | Poll job status and get results |
| `GET` | `/jobs` | List all jobs |
| `GET` | `/` | Health check |
| `GET` | `/docs` | Interactive API docs (Swagger) |

### POST /run

Accepts either bare domain strings or enriched objects with StoreLeads metadata:

```json
{
  "domains": [
    "example.com",
    {"domain": "another.com", "merchant_name": "Another Co", "platform": "shopify", "product_count": 150}
  ]
}
```

Returns `{"job_id": "abc123", "domain_count": 2, "status": "running"}`.

## Pipeline Details

- **Pre-classification** uses StoreLeads platform, categories, and product count to skip obvious non-ecom (news, government, education, etc.) without scraping or LLM calls. Never auto-classifies *as* ecommerce — all potential ecom sites go through full LLM classification.
- **Scraping** uses aiohttp through BrightData residential proxy, 500 concurrent connections, 3 retries with exponential backoff.
- **LLM classification** sends scraped page content + StoreLeads metadata to Claude. Classifies into: `ecommerce`, `software`, `agency`, `services`, `b2b`, `marketplace`, `manufacturer`, `media`, `real_estate`, `finance`, `education`, `nonprofit`, `other`.
- **Amazon matching** searches Serper for `"{merchant_name}" site:amazon.com`, then uses Claude to verify the match. Confidence is 0–5; only confidence >= 2 is reported as a match.
