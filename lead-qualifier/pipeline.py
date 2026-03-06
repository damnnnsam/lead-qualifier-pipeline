#!/usr/bin/env python3
"""
Lead Qualification Pipeline

Accepts enriched domain objects with StoreLeads metadata (platform, categories,
estimated_sales, product_count, merchant_name, etc.) and uses them to:
  - Pre-classify obvious non-ecommerce without scraping or LLM calls
  - Give the LLM richer context for borderline cases
  - Use merchant_name for better Amazon brand matching

Pipeline phases:
  0. Pre-classify using StoreLeads metadata (skip clear non-ecom)
  1. Scrape remaining domains
  2. Classify with LLM (enriched prompt with StoreLeads context)
  3. Amazon presence check (Serper + LLM matching using merchant_name)
"""

import json
import time
import asyncio
import aiohttp
import random
import re
import threading
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from ai.llm_provider import get_llm_client, get_llm_provider_name, resolve_llm_model
from serper.serp_search import SerperSearch
from ai.brand_matcher import BrandMatcher, MATCH_ERROR

# ============================================================================
# CONFIG
# ============================================================================

TIMEOUT = 15
SCRAPE_CONCURRENCY = 500
MAX_RETRIES = 3
RETRY_BASE_DELAY = 0.5

LLM_MODEL = resolve_llm_model("claude-sonnet-4-20250514")
LLM_CONCURRENCY = int(os.getenv("LLM_CONCURRENCY", "500"))

SERP_PARALLEL = 40
LLM_MATCH_PARALLEL = int(os.getenv("LLM_CONCURRENCY", "500"))

PROXY = os.getenv("BRIGHTDATA_PROXY") or os.getenv("BRIGHTDATA_PROXY_URL")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

log_lock = threading.Lock()

def log(msg: str, log_file: Path = None):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with log_lock:
        print(line, flush=True)
        if log_file:
            with open(log_file, "a") as f:
                f.write(line + "\n")


# ============================================================================
# STORELEADS METADATA HELPERS
# ============================================================================

ECOMMERCE_PLATFORMS = {
    "shopify", "woocommerce", "bigcommerce", "magento", "prestashop",
    "opencart", "volusion", "3dcart", "shift4shop", "squarespace",
    "wix", "ecwid", "salesforce commerce", "sap commerce",
}

NON_ECOM_CATEGORY_PATTERNS = [
    r"^/news", r"^/reference", r"^/science", r"^/law",
    r"^/government", r"^/people & society", r"^/education",
    r"^/finance/insurance", r"^/real estate",
    r"^/jobs", r"^/adult",
]

ECOM_CATEGORY_PATTERNS = [
    r"^/shopping", r"^/apparel", r"^/beauty", r"^/food & drink",
    r"^/home & garden", r"^/health/pharmacy", r"^/sports",
    r"^/electronics", r"^/toys", r"^/baby",
]

STRONG_PRODUCT_THRESHOLD = 10


def _has_ecom_platform(meta: dict) -> bool:
    platform = (meta.get("platform") or "").lower().strip()
    return any(p in platform for p in ECOMMERCE_PLATFORMS)


def _category_signal(meta: dict) -> str:
    """Returns 'ecom', 'non_ecom', or 'ambiguous'."""
    cats = (meta.get("categories") or "").lower()
    if not cats:
        return "ambiguous"
    for pat in ECOM_CATEGORY_PATTERNS:
        if re.search(pat, cats):
            return "ecom"
    for pat in NON_ECOM_CATEGORY_PATTERNS:
        if re.search(pat, cats):
            return "non_ecom"
    return "ambiguous"


def _has_products(meta: dict) -> bool:
    pc = meta.get("product_count")
    return pc is not None and pc > 0


def _has_strong_catalog(meta: dict) -> bool:
    pc = meta.get("product_count")
    return pc is not None and pc >= STRONG_PRODUCT_THRESHOLD


def _has_revenue(meta: dict) -> bool:
    rev = meta.get("estimated_sales_monthly") or meta.get("estimated_sales_yearly")
    return rev is not None and rev > 0


# ============================================================================
# PHASE 0: PRE-CLASSIFY WITH STORELEADS METADATA
# ============================================================================

def pre_classify(domains: List[dict], log_file: Path) -> tuple:
    """
    Filter out obvious non-ecommerce domains that aren't worth scraping or classifying.

    NEVER auto-classifies as ecommerce — StoreLeads data can't reliably distinguish
    D2C ecommerce from B2B, manufacturers, or marketplaces. Every potential ecommerce
    domain must go through scrape + LLM classification.

    Only filters: clearly non-ecom categories (news, government, law, education, etc.)
    with tiny or no product catalogs. These aren't leads regardless.

    Returns (needs_llm, filtered_out):
      - needs_llm: domains that need full scrape + LLM classification
      - filtered_out: domains filtered as obvious non-ecommerce
    """
    needs_llm = []
    filtered_out = []

    for d in domains:
        meta = d if isinstance(d, dict) else {"domain": d}
        domain = meta.get("domain", "")

        cat_signal = _category_signal(meta)
        has_strong = _has_strong_catalog(meta)

        if cat_signal == "non_ecom" and not has_strong:
            meta["business_type"] = "not_ecommerce"
            meta["classification_confidence"] = 3
            meta["classification_description"] = (
                f"Filtered: non-ecom category {meta.get('categories', '')}, "
                f"{meta.get('product_count', 0) or 0} products"
            )
            meta["classification_category"] = None
            meta["classified_at"] = datetime.now().isoformat()
            meta["classification_source"] = "storeleads_filter"
            meta["is_online"] = True
            filtered_out.append(meta)
            continue

        needs_llm.append(meta)

    log(
        f"Pre-filter: {len(filtered_out)}/{len(domains)} obvious non-ecom removed. "
        f"{len(needs_llm)} go to scrape+LLM.",
        log_file,
    )
    return needs_llm, filtered_out


# ============================================================================
# PHASE 1: SCRAPE WEBSITES
# ============================================================================

async def scrape_domain(session: aiohttp.ClientSession, domain: str, semaphore: asyncio.Semaphore) -> dict:
    url = f"https://{domain}"
    result = {
        "domain": domain,
        "store_url": url,
        "is_online": False,
        "status_code": None,
        "final_url": None,
        "title": None,
        "meta_description": None,
        "body_text": None,
        "has_cart": False,
        "has_checkout": False,
        "has_add_to_cart": False,
        "has_prices": False,
        "scrape_attempts": 0,
        "scrape_error": None,
        "scraped_at": None,
    }

    async with semaphore:
        for attempt in range(MAX_RETRIES):
            result["scrape_attempts"] = attempt + 1
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                                       allow_redirects=True, ssl=False) as response:
                    result["status_code"] = response.status
                    result["final_url"] = str(response.url)

                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, "html.parser")

                        result["is_online"] = True
                        result["title"] = soup.title.string.strip() if soup.title and soup.title.string else None

                        meta = soup.find("meta", attrs={"name": "description"})
                        result["meta_description"] = meta.get("content", "").strip()[:500] if meta else None

                        body = soup.find("body")
                        if body:
                            text = body.get_text(separator=" ", strip=True)[:5000]
                            result["body_text"] = re.sub(r'\s+', ' ', text)

                        html_lower = html.lower()
                        result["has_cart"] = any(x in html_lower for x in ["/cart", "cart-icon", "shopping-cart", "minicart"])
                        result["has_checkout"] = any(x in html_lower for x in ["/checkout", "checkout-button"])
                        result["has_add_to_cart"] = any(x in html_lower for x in ["add-to-cart", "addtocart", "add to cart"])
                        result["has_prices"] = bool(re.search(r'\$\d+\.?\d*', html))

                        result["scrape_error"] = None
                        result["scraped_at"] = datetime.now().isoformat()
                        return result
                    elif response.status == 403:
                        result["scrape_error"] = "blocked"
                        result["scraped_at"] = datetime.now().isoformat()
                        return result
                    elif response.status >= 500:
                        result["scrape_error"] = f"server_error_{response.status}"
                    else:
                        result["scrape_error"] = f"http_{response.status}"
                        result["scraped_at"] = datetime.now().isoformat()
                        return result

            except asyncio.TimeoutError:
                result["scrape_error"] = "timeout"
            except aiohttp.ClientConnectorError:
                result["scrape_error"] = "connection_error"
            except aiohttp.ClientError as e:
                err_str = str(e)[:100]
                result["scrape_error"] = "ssl_error" if "ssl" in err_str.lower() else err_str
            except Exception as e:
                result["scrape_error"] = str(e)[:100]

            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.5)
                await asyncio.sleep(min(delay, 5))

    result["scraped_at"] = datetime.now().isoformat()
    return result


async def scrape_all(domains_or_metas: List, log_file: Path) -> List[dict]:
    """Scrape all domains. Accepts list of strings or dicts with 'domain' key."""
    domain_list = []
    meta_lookup = {}
    for item in domains_or_metas:
        if isinstance(item, dict):
            d = item["domain"]
            domain_list.append(d)
            meta_lookup[d] = item
        else:
            domain_list.append(item)

    log(f"Scraping {len(domain_list)} domains ({SCRAPE_CONCURRENCY} concurrent)...", log_file)

    connector = (
        ProxyConnector.from_url(PROXY, limit=SCRAPE_CONCURRENCY, ssl=False)
        if PROXY
        else aiohttp.TCPConnector(limit=SCRAPE_CONCURRENCY)
    )
    semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    results = []
    results_lock = asyncio.Lock()
    done_count = [0]
    online_count = [0]

    async def scrape_and_collect(domain: str):
        result = await scrape_domain(session, domain, semaphore)
        if domain in meta_lookup:
            for k, v in meta_lookup[domain].items():
                if k not in result or result[k] is None:
                    result[k] = v
        async with results_lock:
            results.append(result)
            done_count[0] += 1
            if result["is_online"]:
                online_count[0] += 1
            if done_count[0] % 500 == 0 or done_count[0] == len(domain_list):
                log(f"  Scraped {done_count[0]}/{len(domain_list)} - {online_count[0]} online", log_file)

    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        tasks = [scrape_and_collect(d) for d in domain_list]
        await asyncio.gather(*tasks)

    return results


# ============================================================================
# PHASE 2: CLASSIFY BUSINESSES (ENRICHED WITH STORELEADS)
# ============================================================================

def classify_batch(scraped_data: List[dict], log_file: Path) -> List[dict]:
    """Classify businesses using LLM, enriched with StoreLeads metadata."""
    client = get_llm_client()

    online = [d for d in scraped_data if d.get("is_online")]
    offline = [d for d in scraped_data if not d.get("is_online")]

    log(f"Classifying {len(online)} online sites ({LLM_CONCURRENCY} concurrent)...", log_file)

    for d in offline:
        d["business_type"] = "not_ecommerce"
        d["classification_confidence"] = 1
        d["classification_description"] = f"Offline: {d.get('scrape_error', 'unknown')}"
        d["classification_category"] = None
        d["classified_at"] = datetime.now().isoformat()
        d["classification_source"] = "offline"

    semaphore = threading.Semaphore(LLM_CONCURRENCY)

    def build_prompt(data: dict) -> str:
        parts = []

        sl_parts = []
        if data.get("merchant_name"):
            sl_parts.append(f"Merchant: {data['merchant_name']}")
        if data.get("platform"):
            sl_parts.append(f"Platform: {data['platform']}")
        if data.get("categories"):
            sl_parts.append(f"StoreLeads Category: {data['categories']}")
        if data.get("product_count"):
            sl_parts.append(f"Product Count: {data['product_count']}")
        if data.get("estimated_sales_monthly"):
            sl_parts.append(f"Est. Monthly Revenue: ${data['estimated_sales_monthly']:,}")
        if data.get("plan"):
            sl_parts.append(f"Plan: {data['plan']}")
        if data.get("tags"):
            sl_parts.append(f"Tech/Apps: {data['tags']}")
        if data.get("description"):
            sl_parts.append(f"Description: {data['description'][:300]}")
        if sl_parts:
            parts.append("StoreLeads data:\n" + "\n".join(f"  {p}" for p in sl_parts))

        if data.get("title"):
            parts.append(f"Title: {data['title']}")
        if data.get("meta_description"):
            parts.append(f"Meta: {data['meta_description']}")

        signals = []
        if data.get("has_cart"): signals.append("cart")
        if data.get("has_checkout"): signals.append("checkout")
        if data.get("has_add_to_cart"): signals.append("add-to-cart")
        if data.get("has_prices"): signals.append("prices")
        if signals:
            parts.append(f"Ecommerce signals detected: {', '.join(signals)}")

        if data.get("body_text"):
            parts.append(f"Page Content:\n{data['body_text'][:2000]}")

        page_context = "\n\n".join(parts)

        return f"""Classify this website into ONE business type.

Domain: {data['domain']}

{page_context}

BUSINESS TYPES (pick exactly one):

- ecommerce: Online store selling PHYSICAL PRODUCTS directly to consumers (D2C). Must have products you can buy and have shipped. Examples: clothing store, pet supplies shop, electronics retailer, furniture store, beauty products.

- software: SaaS, apps, digital tools, platforms, browser extensions. Companies that BUILD software products.

- agency: Marketing, design, development, SEO, ecommerce growth agencies. Companies that provide SERVICES to other businesses. Even if they use Shopify/WooCommerce for payments, they are agencies.

- services: Professional services for consumers or businesses. Legal, medical, accounting, cleaning, repair, photography, coaching, consulting, salons, spas. NOT selling products.

- b2b: Sells products/equipment to OTHER BUSINESSES, not consumers. Wholesalers, distributors, industrial suppliers. If the site says "commercial", "wholesale", "distributors", it's B2B.

- marketplace: Platform where multiple sellers list products. The site itself doesn't sell - it hosts other sellers.

- manufacturer: Makes/manufactures products but does NOT sell directly to consumers online. May sell through retailers or B2B only.

- media: News sites, blogs, magazines, content publishers, entertainment sites, recipe sites.

- real_estate: Property sales, rentals, real estate agents, property management.

- finance: Banks, insurance, crypto, trading platforms, payment processors, fintech.

- education: Online courses, training platforms, schools, educational content.

- nonprofit: Charities, foundations, NGOs, community organizations.

- other: Doesn't clearly fit any category above.

IMPORTANT RULES:
1. The presence of Shopify/WooCommerce does NOT mean ecommerce. Many agencies, services, and restaurants use these for payments.
2. If a company BUILDS apps, websites, or software for others = agency or software, NOT ecommerce.
3. If it says "services", "solutions", "consulting", "we help", "we build" = likely agency/services/software.
4. D2C ecommerce must sell tangible physical products that get shipped to consumers.
5. A restaurant, salon, gym, or spa is "services" even if it sells gift cards or a few products online.

Respond with JSON only:
{{"business_type": "x", "confidence": 1-3, "description": "one sentence about what this business does", "category": "product category if ecommerce, otherwise null"}}"""

    def classify_one(data: dict) -> dict:
        if (not data.get("title") and not data.get("body_text")
                and not data.get("description") and not data.get("meta_description")):
            data["business_type"] = "not_ecommerce"
            data["classification_confidence"] = 1
            data["classification_description"] = "No content to classify"
            data["classification_category"] = None
            data["classified_at"] = datetime.now().isoformat()
            data["classification_source"] = "no_content"
            return data

        with semaphore:
            prompt = build_prompt(data)
            try:
                response = client.messages.create(
                    model=LLM_MODEL,
                    max_tokens=400,
                    messages=[{"role": "user", "content": prompt}]
                )
                content = response.content[0].text

                json_match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                    data["business_type"] = result.get("business_type", "other")
                    data["classification_confidence"] = result.get("confidence", 1)
                    data["classification_description"] = result.get("description", "")
                    data["classification_category"] = result.get("category", "")
                else:
                    data["business_type"] = "other"
                    data["classification_confidence"] = 1
                    data["classification_description"] = f"Parse failed: {content[:100]}"

            except Exception as e:
                data["business_type"] = "other"
                data["classification_confidence"] = 1
                data["classification_description"] = f"Error: {str(e)[:100]}"

            data["classified_at"] = datetime.now().isoformat()
            data["classification_source"] = "llm"
            return data

    with ThreadPoolExecutor(max_workers=LLM_CONCURRENCY) as executor:
        futures = {executor.submit(classify_one, d): d for d in online}
        done = 0
        for future in as_completed(futures):
            future.result()
            done += 1
            if done % 200 == 0 or done == len(online):
                ecom = sum(1 for d in online if d.get("business_type") == "ecommerce")
                log(f"  Classified {done}/{len(online)} - {ecom} ecommerce", log_file)

    return scraped_data


# ============================================================================
# PHASE 3: AMAZON PRESENCE CHECK (USES MERCHANT_NAME)
# ============================================================================

def check_amazon_presence(ecommerce_domains: List[dict], log_file: Path) -> List[dict]:
    """Check Amazon presence using Serper + LLM matching.
    Uses merchant_name from StoreLeads when available for better brand matching."""
    domains = [d["domain"] for d in ecommerce_domains]
    merchant_names = {
        d["domain"]: d.get("merchant_name") or ""
        for d in ecommerce_domains
        if d.get("merchant_name")
    }
    log(f"Checking Amazon presence for {len(domains)} domains ({len(merchant_names)} with merchant names)...", log_file)

    log(f"  SERP: Searching {len(domains)} domains (Serper @ {SERP_PARALLEL} RPS)...", log_file)
    serper = SerperSearch()
    serp_start = time.time()
    amazon_results = serper.search_batch(domains, merchant_names=merchant_names, max_workers=SERP_PARALLEL)
    serp_time = time.time() - serp_start

    with_results = sum(1 for r in amazon_results.values() if r)
    log(f"  SERP: Complete in {serp_time:.1f}s - {with_results} domains have Amazon results", log_file)

    to_match = [(d, amazon_results.get(d.lower(), [])) for d in domains if amazon_results.get(d.lower())]
    log(f"  LLM: Matching {len(to_match)} domains ({LLM_MATCH_PARALLEL} parallel)...", log_file)

    matcher = BrandMatcher()
    domain_to_result = {d["domain"]: d for d in ecommerce_domains}

    for d in ecommerce_domains:
        d["amazon_status"] = "ecom_only"
        d["amazon_store_url"] = ""
        d["amazon_seller_name"] = ""
        d["amazon_confidence"] = 0
        d["amazon_confidence_reason"] = "No Amazon results"
        d["amazon_checked_at"] = datetime.now().isoformat()

    matched_count = 0
    match_lock = threading.Lock()

    def match_one(domain: str, results: List[Dict]) -> None:
        nonlocal matched_count
        try:
            data = domain_to_result[domain]
            company_name = (
                data.get("merchant_name")
                or BrandMatcher.extract_brand_from_domain(domain)
            )
            match = matcher.match(domain, company_name, results)

            if match is MATCH_ERROR:
                data["amazon_confidence_reason"] = "API error during matching"
                return

            if match and match.confidence >= 2:
                data["amazon_status"] = "on_amazon"
                data["amazon_store_url"] = match.amazon_url
                data["amazon_seller_name"] = match.amazon_seller_name
                data["amazon_confidence"] = match.confidence
                data["amazon_confidence_reason"] = match.confidence_reason
                with match_lock:
                    matched_count += 1
            else:
                data["amazon_confidence_reason"] = "No confident match"
        except Exception as e:
            domain_to_result[domain]["amazon_confidence_reason"] = f"Error: {str(e)[:50]}"

    if to_match:
        llm_start = time.time()
        with ThreadPoolExecutor(max_workers=LLM_MATCH_PARALLEL) as executor:
            futures = {executor.submit(match_one, d, r): d for d, r in to_match}
            done = 0
            for future in as_completed(futures):
                future.result()
                done += 1
                if done % 200 == 0 or done == len(to_match):
                    log(f"    LLM: {done}/{len(to_match)} - {matched_count} on_amazon", log_file)

        llm_time = time.time() - llm_start
        log(f"  LLM: Complete in {llm_time:.1f}s", log_file)

    return ecommerce_domains


# ============================================================================
# MAIN PIPELINE (API ENTRY POINT)
# ============================================================================

def run_pipeline_from_api(domain_metas: List[dict], log_file: Path) -> tuple:
    """
    API entry point. Accepts enriched domain dicts from StoreLeads.
    Returns (all_ecommerce_results, summary_dict).
    """
    total = len(domain_metas)
    log(f"Pipeline start: {total} domains (StoreLeads-enriched)", log_file)
    start_time = time.time()

    # Phase 0: Filter obvious non-ecommerce (news, government, etc.)
    needs_llm, filtered_out = pre_classify(domain_metas, log_file)

    # Phase 1: Scrape domains that need classification
    if needs_llm:
        scraped = asyncio.run(scrape_all(needs_llm, log_file))
        online = sum(1 for s in scraped if s.get("is_online"))
        log(f"Scrape complete: {online}/{len(scraped)} online", log_file)
    else:
        scraped = []

    # Phase 2: Classify ALL scraped domains with LLM — no auto-classification
    if scraped:
        classified = classify_batch(scraped, log_file)
        ecommerce = [d for d in classified if d.get("business_type") == "ecommerce"]
        not_ecommerce = [d for d in classified if d.get("business_type") != "ecommerce"]
    else:
        ecommerce = []
        not_ecommerce = []

    log(
        f"Classification: {len(ecommerce)} ecommerce, "
        f"{len(not_ecommerce)} not ecom, "
        f"{len(filtered_out)} pre-filtered.",
        log_file,
    )

    # Phase 3: Amazon check for all ecommerce
    if ecommerce:
        check_amazon_presence(ecommerce, log_file)

    on_amazon = sum(1 for d in ecommerce if d.get("amazon_status") == "on_amazon")
    ecom_only = sum(1 for d in ecommerce if d.get("amazon_status") == "ecom_only")

    all_results = ecommerce + not_ecommerce + filtered_out
    elapsed = time.time() - start_time
    summary = {
        "input_domains": total,
        "filtered_non_ecom": len(filtered_out),
        "scraped": len(scraped),
        "online": sum(1 for s in scraped if s.get("is_online")) if scraped else 0,
        "ecommerce": len(ecommerce),
        "not_ecommerce": len(not_ecommerce),
        "on_amazon": on_amazon,
        "ecom_only": ecom_only,
        "elapsed_seconds": round(elapsed, 1),
    }
    log(f"Pipeline complete in {elapsed:.1f}s: {json.dumps(summary)}", log_file)
    return all_results, summary
