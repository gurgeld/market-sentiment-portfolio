"""Client utilities for interacting with the Alpha Vantage API."""

from __future__ import annotations

import os
from urllib.parse import urlencode

import requests

from market_sentiment_portfolio.utils.rate_limit import throttle

BASE = "https://www.alphavantage.co/query"

class AlphaVantageClient:
    """Pequeno cliente HTTP para acessar o endpoint de notícias."""

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise RuntimeError("Defina ALPHAVANTAGE_API_KEY")

    @throttle(min_interval_sec=12.5)
    def call(self, **params: str) -> dict:
        params["apikey"] = self.api_key
        url = f"{BASE}?{urlencode(params)}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
