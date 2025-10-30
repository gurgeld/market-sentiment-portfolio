import os
import requests
from urllib.parse import urlencode
from src.utils.rate_limit import throttle

BASE = "https://www.alphavantage.co/query"

class AlphaVantageClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise RuntimeError("Defina ALPHAVANTAGE_API_KEY")

    @throttle(min_interval_sec=12.5)
    def call(self, **params):
        params["apikey"] = self.api_key
        url = f"{BASE}?{urlencode(params)}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
