# Lead Qualifier Pipeline

Company enrichment pipeline that takes domains (with optional StoreLeads metadata) and determines:

1. Whether the business is ecommerce (LLM classification from scraped content)
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
 ├── Phase 1: Scrape all domains via BrightData datacenter proxy (50 concurrent)
 ├── Phase 2: Classify with DeepSeek LLM (enriched prompt w/ metadata + OG/JSON-LD)
 │             └── StoreLeads fallback for scrape failures with strong ecom signals
 └── Phase 3: Amazon presence check (dual Serper SERP query + DeepSeek brand matching)
```

## Project Structure

```
lead-qualifier-pipeline/
├── lead-qualifier/              # Deployable unit (Fly.io)
│   ├── api.py                   # FastAPI — POST /run, GET /status/{job_id}, GET /jobs
│   ├── pipeline.py              # Core pipeline (scrape, classify, Amazon check)
│   ├── ai/
│   │   ├── llm_provider.py      # DeepSeek/Claude LLM client abstraction
│   │   └── brand_matcher.py     # Amazon brand matching via LLM
│   ├── serper/
│   │   └── serp_search.py       # Serper.dev SERP API wrapper (dual-query)
│   ├── Dockerfile
│   ├── fly.toml                 # Fly.io config (performance-8x, 32 GB, scale-to-zero)
│   ├── requirements.txt
│   └── .env.example
├── run_api_cloud.py             # Client: sends domain batches to the API
├── generate_next_batch.py       # Generates batch JSON from storeleads_dump.sql.gz
├── upload_results_to_bq.py      # Uploads JSONL/CSV results to BigQuery
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

### 3. Upload results to BigQuery

```bash
python upload_results_to_bq.py results_batch7.jsonl
```

Uploads to `ecom_brain.api_results`. Deduplicates against existing rows by domain.

### 4. Results format

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
  "classification_source": "llm",
  "niche": "premium pet food & accessories",
  "amazon_status": "on_amazon",
  "amazon_store_url": "https://www.amazon.com/stores/Example/page/...",
  "amazon_seller_name": "Example Pet Co",
  "amazon_confidence": 4,
  "amazon_confidence_reason": "Exact brand name match on Amazon storefront",
  "amazon_checked_at": "2026-03-08T12:00:00"
}
```

**`business_type`** values: `ecommerce`, `software`, `agency`, `services`, `b2b`, `marketplace`, `manufacturer`, `media`, `real_estate`, `finance`, `education`, `nonprofit`, `other`, `not_ecommerce`, `scrape_failed`.

**`classification_source`** values: `llm` (scraped + classified), `storeleads_fallback` (scrape failed but strong StoreLeads ecom signals), `scrape_failed` (scrape failed, no fallback data), `no_content` (site online but empty).

**`amazon_status`** values: `on_amazon` (matched with confidence >= 2), `ecom_only` (ecommerce but not found on Amazon). Only set for ecommerce domains.

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

- **Every domain gets the full pipeline** — no pre-filtering. This ensures every domain gets an AI-generated niche and description for email personalization, plus an Amazon check. StoreLeads metadata is passed to the LLM as supplementary context, not used to skip domains.
- **Scrape failure fallback**: Domains that fail scraping (403, timeout, proxy error) are checked against StoreLeads metadata. If the domain has a known ecom platform, products > 0, and revenue > 0, it's classified as `ecommerce` via `storeleads_fallback`. Otherwise it gets `scrape_failed`. This prevents misclassifying blocked ecom sites.
- **Scraping** uses aiohttp through BrightData datacenter proxy (50 concurrent, configurable via `SCRAPE_CONCURRENCY`). 3 retries with exponential backoff. Response size capped at 2 MB, overall phase timeout of 10 minutes. Retries on HTTP 429 and 5xx.
- **JS-rendered site handling**: For sites that return empty body text (SPAs, Shopify Hydrogen, etc.), the scraper extracts OG meta tags (`og:title`, `og:description`), JSON-LD structured data, and detects ecommerce platforms (Shopify, WooCommerce, BigCommerce, Magento) from HTML signals.
- **LLM classification** sends scraped content + StoreLeads metadata + structured data to DeepSeek. Classifies into 13 business types. Includes retry with exponential backoff on rate limits, 30s timeout per call.
- **Amazon matching** uses a dual-query Serper search: both `"{merchant_name}" site:amazon.com` and `"{domain_brand}" site:amazon.com` when the two differ. Results are merged and deduplicated, then DeepSeek verifies the best match. Confidence is 0–5; only confidence >= 2 is reported as a match. This dual approach catches cases where the StoreLeads merchant name has suffixes ("etnies US"), special characters ("böhme"), or differs significantly from the brand name consumers search for.
- `MATCH_ERROR` sentinel from `brand_matcher.py` must be checked before accessing `.confidence` on match results.
