#!/usr/bin/env python3
"""
AI Brand Matcher

Use LLM to match Amazon seller names to company/brand names.
Supports both individual requests (with retry) and batch processing.
"""

import json
import os
import re
import time
import threading
from typing import List, Dict, Optional
from dataclasses import dataclass
from .llm_provider import get_llm_client, get_llm_provider_name, resolve_llm_model


@dataclass
class BrandMatch:
    """Result of brand matching"""
    domain: str
    company_name: str
    amazon_url: str
    amazon_seller_name: str
    confidence: int  # 0-5 scale (0=no match, 2=possible, 3=probable, 4=confident, 5=exact), -1 = API error
    confidence_reason: str
    
    @property
    def is_error(self) -> bool:
        """Returns True if this result represents an API error"""
        return self.confidence == -1


# Sentinel value for API failures (distinguishes from "no match")
MATCH_ERROR = "MATCH_ERROR"


_llm_semaphore = threading.Semaphore(int(os.environ.get("LLM_CONCURRENCY", "50")))


class BrandMatcher:
    """Match Amazon sellers to company names using LLM provider"""

    def __init__(self, model: str = None):
        self.llm = get_llm_client()
        self.provider = get_llm_provider_name()
        self.model = resolve_llm_model(model, provider=self.provider)

    def _build_prompt(self, domain: str, company_name: str, amazon_results: List[Dict]) -> str:
        """Build the matching prompt for a domain"""
        amazon_listings = "\n".join([
            f"- URL: {r['url']}\n  Title: {r['title']}\n  Description: {r.get('description', 'N/A')}"
            for r in amazon_results[:10]
        ])
        
        return f"""Does this company sell on Amazon?

Company: "{company_name}" (domain: {domain})

Amazon search results:
{amazon_listings}

Find the SINGLE best matching Amazon listing for this company. Consider:
- Exact name matches ("PetSafe" = "PetSafe")
- Spacing/casing variants ("PetSafe" = "Pet Safe" = "PETSAFE")
- With/without suffixes ("Acme" = "Acme Inc" = "Acme LLC")
- Parent/subsidiary brands
- Domain name matching seller name

Only consider actual Amazon seller pages, store pages, or brand pages. Ignore help pages, forums, or unrelated products.

Respond with JSON only:
{{
    "url": "amazon url or empty string if no match",
    "seller_name": "seller/brand name from listing or empty string",
    "confidence": 0-5,
    "reason": "brief explanation"
}}

Confidence: 0=no match found, 1=unlikely, 2=possible, 3=probable, 4=confident, 5=exact match."""

    def _parse_response(self, domain: str, company_name: str, content: str) -> Optional[BrandMatch]:
        """Parse LLM response and extract match"""
        cleaned = re.sub(r"```(?:json)?\s*", "", content).strip().rstrip("`")
        json_match = re.search(r'\{[\s\S]*\}', cleaned)
        if not json_match:
            return None

        try:
            result = json.loads(json_match.group())
        except (json.JSONDecodeError, ValueError):
            return None

        if "matches" in result:
            matches = result["matches"]
            if not matches:
                return None
            best = max(matches, key=lambda x: x.get("confidence", 0))
            result = best

        if result.get("confidence", 0) >= 2 and result.get("url"):
            return BrandMatch(
                domain=domain,
                company_name=company_name,
                amazon_url=result.get("url", ""),
                amazon_seller_name=result.get("seller_name", ""),
                confidence=result.get("confidence", 1),
                confidence_reason=result.get("reason", "")
            )
        return None

    def match(self, domain: str, company_name: str, amazon_results: List[Dict], max_retries: int = 3):
        """
        Determine if any Amazon listing matches the company.
        Includes retry logic with exponential backoff for rate limit errors.
        
        Args:
            domain: The company domain (e.g., petique.com)
            company_name: The company name
            amazon_results: List of Amazon URLs with titles/descriptions
            max_retries: Number of retries on failure (default 3)
            
        Returns:
            BrandMatch if confident match found
            None if no match found
            MATCH_ERROR string if API failed (so caller can distinguish from "no match")
        """
        if not amazon_results:
            return None
        
        prompt = self._build_prompt(domain, company_name, amazon_results)
        
        for attempt in range(max_retries):
            try:
                with _llm_semaphore:
                    response = self.llm.messages.create(
                        model=self.model,
                        max_tokens=300,
                        timeout=30,
                        messages=[{"role": "user", "content": prompt}]
                    )
                
                content = response.content[0].text
                return self._parse_response(domain, company_name, content)
                
            except Exception as e:
                error_str = str(e)
                is_rate_limit = "429" in error_str or "rate_limit" in error_str.lower()
                
                if is_rate_limit and attempt < max_retries - 1:
                    # Exponential backoff: 2s, 4s, 8s
                    wait_time = 2 ** (attempt + 1)
                    print(f"[{self.provider.upper()}] Rate limit for {domain}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif attempt < max_retries - 1:
                    # Other errors: shorter backoff
                    time.sleep(1)
                    continue
                else:
                    print(f"[{self.provider.upper()}] API ERROR for {domain} (after {max_retries} attempts): {e}")
                    return MATCH_ERROR
        
        return MATCH_ERROR

    def match_batch(
        self, 
        requests: List[Dict],  # [{"domain": str, "company_name": str, "amazon_results": List[Dict]}]
        poll_interval: float = 5.0,
        timeout: float = 3600  # 1 hour max
    ) -> Dict[str, any]:
        """
        Process multiple brand matching requests using Anthropic's Batch API.
        
        Args:
            requests: List of dicts with domain, company_name, amazon_results
            poll_interval: Seconds between status checks (default 5s)
            timeout: Maximum wait time in seconds (default 1 hour)
            
        Returns:
            Dict mapping domain -> BrandMatch (match found), None (no match), or MATCH_ERROR (API failed)
        """
        if not requests:
            return {}
        
        # Filter out requests with no amazon_results
        valid_requests = [r for r in requests if r.get('amazon_results')]
        
        if not valid_requests:
            return {r['domain']: None for r in requests}
        
        # DEDUPLICATE: Only keep first occurrence of each domain
        seen_domains = set()
        deduped_requests = []
        duplicate_count = 0
        
        for req in valid_requests:
            domain = req['domain'].lower()
            if domain not in seen_domains:
                seen_domains.add(domain)
                deduped_requests.append(req)
            else:
                duplicate_count += 1
        
        if duplicate_count > 0:
            print(f"[{self.provider.upper()} Batch] WARNING: Removed {duplicate_count} duplicate domains")
        
        valid_requests = deduped_requests
        
        # Build batch requests
        # custom_id must match ^[a-zA-Z0-9_-]{1,64}$ so we sanitize domains
        def sanitize_id(domain: str) -> str:
            """Convert domain to valid custom_id (replace dots with underscores, truncate to 64 chars)"""
            return domain.replace('.', '_').replace(':', '_').replace('-', '_')[:64]
        
        # Map sanitized IDs back to domains
        id_to_domain = {}
        batch_requests = []
        
        for req in valid_requests:
            domain = req['domain']
            company_name = req['company_name']
            amazon_results = req['amazon_results']
            
            custom_id = sanitize_id(domain)
            id_to_domain[custom_id] = domain
            
            prompt = self._build_prompt(domain, company_name, amazon_results)
            
            batch_requests.append({
                "custom_id": custom_id,
                "params": {
                    "model": self.model,
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}]
                }
            })
        
        print(f"[{self.provider.upper()} Batch] Creating batch with {len(batch_requests)} requests...")
        
        try:
            # Create the batch
            batch = self.llm.messages.batches.create(requests=batch_requests)
            batch_id = batch.id
            print(f"[{self.provider.upper()} Batch] Batch created: {batch_id}")
            
            # Poll for completion
            start_time = time.time()
            while True:
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    print(f"[{self.provider.upper()} Batch] TIMEOUT after {timeout}s - marking all as errors")
                    return {r['domain']: MATCH_ERROR for r in requests}
                
                status = self.llm.messages.batches.retrieve(batch_id)
                
                if status.processing_status == "ended":
                    print(f"[{self.provider.upper()} Batch] Batch completed in {elapsed:.1f}s")
                    break
                
                # Show progress
                counts = status.request_counts
                total = counts.processing + counts.succeeded + counts.errored + counts.canceled + counts.expired
                done = counts.succeeded + counts.errored
                print(f"[{self.provider.upper()} Batch] Progress: {done}/{total} ({counts.succeeded} succeeded, {counts.errored} errored)")
                
                time.sleep(poll_interval)
            
            # Collect results
            results = {}
            
            # Initialize all domains as None (no SERP results = genuinely not on Amazon)
            for req in requests:
                results[req['domain']] = None
            
            # Track successes and failures
            succeeded = 0
            failed = 0
            
            # Process batch results
            for result in self.llm.messages.batches.results(batch_id):
                custom_id = result.custom_id
                domain = id_to_domain.get(custom_id, custom_id)
                
                if result.result.type == "succeeded":
                    content = result.result.message.content[0].text
                    company_name = next(
                        (r['company_name'] for r in valid_requests if r['domain'] == domain),
                        self.extract_brand_from_domain(domain)
                    )
                    results[domain] = self._parse_response(domain, company_name, content)
                    succeeded += 1
                else:
                    print(f"[{self.provider.upper()} Batch] FAILED for {domain}: {result.result.type}")
                    results[domain] = MATCH_ERROR
                    failed += 1
            
            print(f"[{self.provider.upper()} Batch] Results: {succeeded} succeeded, {failed} failed")
            return results
            
        except Exception as e:
            error_msg = str(e)
            print(f"[{self.provider.upper()} Batch] BATCH ERROR: {error_msg[:500]}")
            
            # Mark ALL domains as errors so they can be retried
            error_results = {}
            for req in requests:
                error_results[req['domain']] = MATCH_ERROR
            
            print(f"[{self.provider.upper()} Batch] Marked {len(error_results)} domains as MATCH_ERROR for retry")
            return error_results

    @staticmethod
    def extract_brand_from_domain(domain: str) -> str:
        """Extract likely brand name from domain (e.g., petique.com -> Petique)"""
        name = domain.lower()
        name = re.sub(r'^www\.', '', name)
        name = re.sub(r'\.(com|net|org|io|co|us|uk|ca|shop|store|pet|dog|pets)$', '', name)
        return name.title()

