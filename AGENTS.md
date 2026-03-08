# Lead Qualifier Pipeline — Agent Context

## What this is

A REST API (FastAPI on Fly.io) that takes a list of domains and returns:
- **Business classification**: ecommerce, software, agency, services, b2b, etc. (every domain gets scraped + LLM classified, no pre-filtering)
- **Niche identification**: a specific 2-6 word niche for email personalization (e.g. "premium organic baby skincare")
- **Amazon presence**: whether the brand sells on Amazon, with store URL and confidence score

## API Contract

**Base URL**: `https://lead-qualifier-pipeline.fly.dev`

All endpoints require an `X-API-Key` header.

### POST /run — Submit domains for processing

```json
{
  "domains": [
    "example.com",
    {"domain": "another.com", "merchant_name": "Another Co", "platform": "shopify", "product_count": 150}
  ]
}
```

Domains can be bare strings or enriched objects with optional StoreLeads metadata fields:
`merchant_name`, `platform`, `categories`, `product_count`, `estimated_sales_monthly`, `plan`, `tags`, `description`.

Response: `{"job_id": "abc123", "domain_count": 2, "status": "running"}`

### GET /status/{job_id} — Poll for results

Response when complete:

```json
{
  "job_id": "abc123",
  "status": "complete",
  "results": [
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
  ]
}
```

### GET /jobs — List all jobs

### GET / — Health check

## Result Fields

| Field | Type | Description |
|---|---|---|
| `domain` | string | The input domain |
| `title` | string\|null | Page title from scrape |
| `business_type` | string | One of: `ecommerce`, `software`, `agency`, `services`, `b2b`, `marketplace`, `manufacturer`, `media`, `real_estate`, `finance`, `education`, `nonprofit`, `other`, `not_ecommerce`, `scrape_failed` |
| `is_online` | bool | Whether the site was reachable |
| `scrape_error` | string\|null | Error details if scrape failed |
| `brd_error` | string\|null | BrightData proxy error if applicable |
| `classification_category` | string\|null | Product category (ecommerce only) |
| `classification_description` | string | One-sentence AI-generated summary of what the business does |
| `classification_source` | string | How the classification was made: `llm`, `storeleads_fallback`, `scrape_failed`, or `no_content` |
| `niche` | string | Specific 2-6 word niche for email personalization (e.g. "handmade boho jewelry") |
| `amazon_status` | string\|null | `on_amazon` or `ecom_only`. Only set for ecommerce domains |
| `amazon_store_url` | string\|null | Amazon store/brand page URL |
| `amazon_seller_name` | string\|null | Matched seller name on Amazon |
| `amazon_confidence` | int\|null | 0-5 confidence score (only >= 2 reported as match) |
| `amazon_confidence_reason` | string\|null | Why the match was made |
| `amazon_checked_at` | string\|null | ISO timestamp of Amazon check |

## Integration Pattern

The API is async. Submit a batch, then poll for results:

```python
import httpx, time

API = "https://lead-qualifier-pipeline.fly.dev"
KEY = "your-api-key"
HEADERS = {"X-API-Key": KEY}

# 1. Submit
resp = httpx.post(f"{API}/run", json={"domains": domains}, headers=HEADERS)
job_id = resp.json()["job_id"]

# 2. Poll
while True:
    status = httpx.get(f"{API}/status/{job_id}", headers=HEADERS).json()
    if status["status"] in ("complete", "error"):
        break
    time.sleep(10)

results = status["results"]  # list of dicts with fields above
```

For large batches (10K+ domains), split into chunks of ~750 and submit concurrently (up to 16 parallel jobs). The included `run_api_cloud.py` client does this automatically.

## Limits

- Max 5000 domains per request (configurable via `MAX_DOMAINS_PER_REQUEST`)
- Recommended batch size: 750 (balances throughput and memory)
- Per-job timeout: 1200s (configurable via `JOB_TIMEOUT_SECONDS`)
- Scale-to-zero: first request after idle takes ~10s for VM to boot

## Key Files

| File | Purpose |
|---|---|
| `lead-qualifier/api.py` | FastAPI app with all endpoints |
| `lead-qualifier/pipeline.py` | Core pipeline: scrape, classify, Amazon check |
| `lead-qualifier/ai/llm_provider.py` | LLM client abstraction (DeepSeek/Claude) |
| `lead-qualifier/ai/brand_matcher.py` | Amazon brand matching via LLM |
| `lead-qualifier/serper/serp_search.py` | Serper.dev SERP API wrapper (dual-query) |
| `run_api_cloud.py` | Batch client for sending domains to the API |
| `upload_results_to_bq.py` | Uploads JSONL/CSV results to BigQuery |
| `generate_next_batch.py` | Generates batch JSON from StoreLeads dump |
| `compare_ab.py` | A/B test: compare old vs new pipeline results |

## Environment Variables

See `lead-qualifier/.env.example` for the full list. The critical ones for integration:

- `API_KEY` — auth token for the API (set as `X-API-Key` header)
- `DEEPSEEK_API_KEY` — LLM provider
- `SERPER_API_KEY` — SERP search for Amazon matching
- `BRIGHTDATA_PROXY` — web scraping proxy
