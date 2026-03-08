#!/usr/bin/env python3
"""
Serper.dev SERP Search API

Fast, instant SERP results with up to 50 RPS.
"""

import json
import http.client
import os
import time
import threading
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


class SerperSearch:
    """Google SERP search via Serper.dev API"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("SERPER_API_KEY")
        self.host = "google.serper.dev"
        
        # Rate limiting: 50 RPS max, use 40 to be safe
        self._rate_lock = threading.Lock()
        self._request_times = []
        self._max_rps = 40
    
    def _rate_limit(self):
        """Rate limit to 40 requests per second"""
        while True:
            with self._rate_lock:
                now = time.time()
                self._request_times = [t for t in self._request_times if now - t < 1.0]
                if len(self._request_times) < self._max_rps:
                    self._request_times.append(now)
                    return
            time.sleep(0.03)
    
    def search(self, query: str, location: str = "United States", _retries: int = 2) -> Dict:
        """
        Perform a single SERP search with retry on transient errors.
        
        Args:
            query: Search query
            location: Location for search results
            _retries: Internal retry count for transient failures
            
        Returns:
            Dict with search results
        """
        self._rate_limit()
        
        conn = http.client.HTTPSConnection(self.host, timeout=10)
        payload = json.dumps({
            "q": query,
            "location": location
        })
        headers = {
            'X-API-KEY': self.api_key,
            'Content-Type': 'application/json'
        }
        
        try:
            conn.request("POST", "/search", payload, headers)
            res = conn.getresponse()
            status = res.status
            data = res.read()
            
            if status >= 500 and _retries > 0:
                conn.close()
                time.sleep(1)
                return self.search(query, location, _retries=_retries - 1)
            
            if status != 200:
                print(f"[Serper] HTTP {status}: {data.decode('utf-8')[:200]}")
                return {"error": f"HTTP {status}"}
            
            return json.loads(data.decode("utf-8"))
        except (TimeoutError, OSError, http.client.HTTPException) as e:
            conn.close()
            if _retries > 0:
                time.sleep(1)
                return self.search(query, location, _retries=_retries - 1)
            print(f"[Serper] Exception: {e}")
            return {"error": str(e)}
        except Exception as e:
            print(f"[Serper] Exception: {e}")
            return {"error": str(e)}
        finally:
            conn.close()
    
    def search_amazon_seller(self, domain: str, merchant_name: str = None) -> List[Dict]:
        """
        Search for a domain's Amazon seller/store presence.
        
        Runs up to two queries when merchant_name differs from the domain-derived
        brand (e.g. "etnies US" vs "etnies"), then merges and deduplicates results.
        This catches cases where one query variant finds results the other misses.
        """
        brand = domain.split(".")[0]
        brand_query = f'"{brand}" site:amazon.com'

        queries = [brand_query]
        if merchant_name and merchant_name.strip().lower() != brand.lower():
            queries.insert(0, f'"{merchant_name.strip()}" site:amazon.com')

        seen_urls = set()
        amazon_results = []

        for query in queries:
            result = self.search(query)
            if "error" in result:
                continue
            for item in result.get("organic", []):
                link = item.get("link", "")
                if "amazon.com" in link.lower() and link not in seen_urls:
                    seen_urls.add(link)
                    amazon_results.append({
                        "url": link,
                        "title": item.get("title", ""),
                        "description": item.get("snippet", "")
                    })

        return amazon_results[:10]
    
    def search_batch(self, domains: List[str], merchant_names: Dict[str, str] = None, max_workers: int = 40) -> Dict[str, List[Dict]]:
        """
        Search multiple domains in parallel.
        
        Args:
            domains: List of domains to search
            merchant_names: Optional dict mapping domain -> merchant_name
            max_workers: Max parallel requests (default 40, under 50 RPS limit)
            
        Returns:
            Dict mapping domain -> amazon_results list
        """
        merchant_names = merchant_names or {}
        results = {}
        total = len(domains)
        completed = 0
        lock = threading.Lock()
        
        def search_one(domain: str) -> tuple:
            nonlocal completed
            merchant = merchant_names.get(domain) or merchant_names.get(domain.lower())
            amazon_results = self.search_amazon_seller(domain, merchant_name=merchant)
            with lock:
                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"  [Serper] {completed}/{total} domains searched")
            return domain, amazon_results
        
        print(f"[Serper] Searching {total} domains (up to {max_workers} parallel)...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(search_one, d): d for d in domains}
            
            for future in as_completed(futures):
                try:
                    domain, amazon_results = future.result()
                    results[domain.lower()] = amazon_results
                except Exception as e:
                    domain = futures[future]
                    print(f"  [Serper] Error for {domain}: {e}")
                    results[domain.lower()] = []
        
        with_results = sum(1 for v in results.values() if v)
        print(f"  [Serper] Complete: {with_results}/{total} domains have Amazon results")
        
        return results

