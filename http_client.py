"""
http_client.py – Global HTTP/2 async client with connection pooling.

ONE httpx.AsyncClient for the entire process, created at startup, reused
everywhere.  Eliminates repeated TLS handshakes via HTTP/2 keep-alive.

After the first request to a given origin the TLS session is reused,
dropping per-request latency from ~128ms to ~22-24ms.
"""
from __future__ import annotations

import httpx

_client: httpx.AsyncClient | None = None


async def init_client() -> httpx.AsyncClient:
    """Create the global async HTTP/2 client.  Call once at startup."""
    global _client
    if _client is not None:
        return _client
    _client = httpx.AsyncClient(
        http2=True,
        timeout=httpx.Timeout(5.0),
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
        ),
        headers={"User-Agent": "poly-bot/1.0"},
    )
    return _client


def get_client() -> httpx.AsyncClient:
    """Return the global client.  Raises if init_client() wasn't called."""
    if _client is None:
        raise RuntimeError("HTTP client not initialised – call init_client() first")
    return _client


async def close_client() -> None:
    """Gracefully close the global client.  Call once at shutdown."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
