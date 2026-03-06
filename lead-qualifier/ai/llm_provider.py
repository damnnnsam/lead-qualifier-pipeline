#!/usr/bin/env python3
"""
LLM provider abstraction for Claude and DeepSeek.
"""

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


DEFAULT_MODELS = {
    "claude": "claude-sonnet-4-20250514",
    "deepseek": "deepseek-chat",
}


def get_llm_provider_name() -> str:
    provider = (os.getenv("LLM_PROVIDER") or "deepseek").strip().lower()
    if provider in ("anthropic", "claude"):
        return "claude"
    if provider in ("deepseek", "deep-seek"):
        return "deepseek"
    return "deepseek"


def resolve_llm_model(requested_model: Optional[str] = None, provider: Optional[str] = None) -> str:
    provider = provider or get_llm_provider_name()
    if requested_model:
        if provider == "claude" and requested_model.startswith("claude-"):
            return requested_model
        if provider == "deepseek" and requested_model.startswith("deepseek-"):
            return requested_model
    return DEFAULT_MODELS.get(provider, DEFAULT_MODELS["claude"])


def _get_concurrency() -> int:
    value = os.getenv("LLM_CONCURRENCY", "10")
    try:
        return max(1, int(value))
    except ValueError:
        return 10


@dataclass
class LLMContent:
    text: str


@dataclass
class LLMMessage:
    content: List[LLMContent]


@dataclass
class LLMRequestCounts:
    processing: int
    succeeded: int
    errored: int
    canceled: int = 0
    expired: int = 0


@dataclass
class LLMBatchStatus:
    id: str
    processing_status: str
    request_counts: LLMRequestCounts


@dataclass
class LLMResultPayload:
    type: str
    message: Optional[LLMMessage] = None


@dataclass
class LLMBatchResult:
    custom_id: str
    result: LLMResultPayload


class DeepSeekMessages:
    def __init__(self, client, semaphore: threading.Semaphore):
        self._client = client
        self._semaphore = semaphore

    def create(self, model: str, max_tokens: int, messages: List[Dict], timeout: Optional[float] = None):
        model = resolve_llm_model(model, provider="deepseek")
        kwargs = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": messages,
        }
        if timeout is not None:
            kwargs["timeout"] = timeout
        with self._semaphore:
            response = self._client.chat.completions.create(**kwargs)
        text = response.choices[0].message.content or ""
        return LLMMessage(content=[LLMContent(text=text)])


class DeepSeekBatches:
    def __init__(self, messages: DeepSeekMessages):
        self._messages = messages
        self._lock = threading.Lock()
        self._store: Dict[str, Dict[str, object]] = {}

    def create(self, requests: List[Dict]):
        batch_id = f"deepseek_batch_{uuid.uuid4().hex}"
        results: List[LLMBatchResult] = []

        def run_request(req: Dict) -> LLMBatchResult:
            custom_id = req.get("custom_id", "")
            params = req.get("params", {})
            try:
                message = self._messages.create(
                    model=params.get("model"),
                    max_tokens=params.get("max_tokens", 512),
                    messages=params.get("messages", []),
                )
                return LLMBatchResult(
                    custom_id=custom_id,
                    result=LLMResultPayload(type="succeeded", message=message),
                )
            except Exception:
                return LLMBatchResult(
                    custom_id=custom_id,
                    result=LLMResultPayload(type="errored", message=None),
                )

        max_workers = min(_get_concurrency(), max(1, len(requests)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(run_request, req) for req in requests]
            for future in as_completed(futures):
                results.append(future.result())

        counts = LLMRequestCounts(
            processing=0,
            succeeded=sum(1 for r in results if r.result.type == "succeeded"),
            errored=sum(1 for r in results if r.result.type != "succeeded"),
        )
        status = LLMBatchStatus(
            id=batch_id,
            processing_status="ended",
            request_counts=counts,
        )

        with self._lock:
            self._store[batch_id] = {
                "status": status,
                "results": results,
            }

        return status

    def retrieve(self, batch_id: str):
        with self._lock:
            batch = self._store.get(batch_id)
        if not batch:
            raise ValueError(f"Unknown DeepSeek batch_id: {batch_id}")
        return batch["status"]

    def results(self, batch_id: str) -> Iterable[LLMBatchResult]:
        with self._lock:
            batch = self._store.get(batch_id)
        if not batch:
            raise ValueError(f"Unknown DeepSeek batch_id: {batch_id}")
        return list(batch["results"])


class DeepSeekClient:
    def __init__(self, api_key: Optional[str] = None, http_client=None):
        from openai import OpenAI

        key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self._client = OpenAI(
            api_key=key,
            base_url="https://api.deepseek.com",
            http_client=http_client,
        )
        self._semaphore = threading.Semaphore(_get_concurrency())
        self.messages = DeepSeekMessages(self._client, self._semaphore)
        self.messages.batches = DeepSeekBatches(self.messages)


def get_llm_client(*, api_key: Optional[str] = None, http_client=None):
    provider = get_llm_provider_name()
    if provider == "deepseek":
        return DeepSeekClient(api_key=api_key, http_client=http_client)

    from anthropic import Anthropic
    return Anthropic(api_key=api_key, http_client=http_client)
