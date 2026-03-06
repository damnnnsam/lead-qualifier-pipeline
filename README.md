# Lead Qualifier Pipeline

Company enrichment pipeline that takes domains (with optional StoreLeads metadata) and determines:

1. Whether the business is ecommerce (pre-classification + LLM classification)
2. Whether the business sells on Amazon (Serper SERP search + LLM brand matching)

Output: qualified lead list with domain, business type, Amazon store URL, seller name, and confidence score.

## Architecture

```
Client (run_api_cloud.py)
 │  HTTP POST /run — 750 domains per batch, 16 concurrent jobs
 ▼
FastAPI API (lead-qualifier/api.py) ── deployed on Fly.io as "lead-qualifier-pipeline"
 │
 ▼
Pipeline (lead-qualifier/pipeline.py)
 ├── Phase 0: Pre-classify using StoreLeads metadata (skip obvious non-ecom)
 ├── Phase 1: Scrape via BrightData datacenter proxy (50 concurrent, configurable)
 ├── Phase 2: Classify with DeepSeek LLM (enriched prompt w/ metadata + OG/JSON-LD)
 └── Phase 3: Amazon presence check (Serper SERP + DeepSeek brand matching)
```

## Project Structure

```
lead-qualifier-pipeline/
├── lead-qualifier/              # Deployable unit (Fly.io)
│   ├── api.py                   # FastAPI — POST /run, GET /status/{job_id}, GET /jobs
│   ├── pipeline.py              # Core pipeline (pre-classify, scrape, classify, Amazon check)
│   ├── ai/
│   │   ├── llm_provider.py      # DeepSeek/Claude LLM client abstraction
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
| `DEEPSEEK_API_KEY` | Yes | DeepSeek API access |
| `SERPER_API_KEY` | Yes | Serper.dev SERP search |
| `API_KEY` | Yes | Auth token for the pipeline API |
| `BRIGHTDATA_PROXY` | No | BrightData proxy URL (HTTP format) |
| `LLM_PROVIDER` | No | `deepseek` (default) or `claude` |
| `LLM_CONCURRENCY` | No | Concurrent LLM calls (default: 50) |
| `SCRAPE_CONCURRENCY` | No | Concurrent scrape connections (default: 50) |
| `MAX_CONCURRENT_JOBS` | No | Max parallel pipeline jobs (default: 16) |
| `MAX_DOMAINS_PER_REQUEST` | No | Max domains per API request (default: 5000) |
| `JOB_TIMEOUT_SECONDS` | No | Per-job timeout (default: 1200) |
| `ANTHROPIC_API_KEY` | No | Only needed if `LLM_PROVIDER=claude` |

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
fly secrets set DEEPSEEK_API_KEY=sk-... SERPER_API_KEY=... API_KEY=... BRIGHTDATA_PROXY=http://...
```

The VM is `performance-8x` / 32 GB RAM and scales to zero when idle. Fly keeps release history so you can roll back with `fly releases` and `fly deploy --image`.

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

The client sends 750-domain batches (16 concurrent), polls for results, and appends to the JSONL results file. It's resumable — already-processed domains are skipped on restart.

### 3. Results format

Each line in the results JSONL file:

```json
{
  "domain": "example.com",
  "title": "Example Pet Store",
  "business_type": "ecommerce",
  "is_online": true,
  "scrape_error": null,
  "brd_error": null,
  "classification_category": "Pet Supplies",
  "classification_description": "Online pet supply store selling food, toys, and accessories",
  "niche": "premium pet food & accessories",
  "amazon_status": "on_amazon",
  "amazon_store_url": "https://www.amazon.com/stores/Example/page/...",
  "amazon_seller_name": "Example Pet Co",
  "amazon_confidence": 4,
  "amazon_confidence_reason": "Exact brand name match on Amazon storefront"
}
```

`business_type` values: `ecommerce`, `software`, `agency`, `services`, `b2b`, `marketplace`, `manufacturer`, `media`, `real_estate`, `finance`, `education`, `nonprofit`, `other`, `scrape_failed`, `offline`.

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
- **Scraping** uses aiohttp through BrightData datacenter proxy (50 concurrent, configurable via `SCRAPE_CONCURRENCY`). 3 retries with exponential backoff. Response size capped at 2 MB, overall phase timeout of 10 minutes. Retries on HTTP 429 and 5xx.
- **JS-rendered site handling**: For sites that return empty body text (SPAs, Shopify Hydrogen, etc.), the scraper extracts OG meta tags (`og:title`, `og:description`), JSON-LD structured data, and detects ecommerce platforms (Shopify, WooCommerce, BigCommerce, Magento) from HTML signals. This recovers ~75% of otherwise-unclassifiable JS-rendered sites.
- **LLM classification** sends scraped content + StoreLeads metadata + structured data to DeepSeek. Classifies into 13 business types. Includes retry with exponential backoff on rate limits, 30s timeout per call.
- **Amazon matching** searches Serper for `"{merchant_name}" site:amazon.com`, then uses DeepSeek to verify the match. Confidence is 0–5; only confidence >= 2 is reported as a match. Serper calls have retry on transient errors.
- **Error reporting**: Domains that fail during scraping get accurate `business_type` labels (`scrape_failed` for proxy/timeout issues, `offline` for genuinely dead sites) with detailed `scrape_error` and `brd_error` fields instead of being silently misclassified.
