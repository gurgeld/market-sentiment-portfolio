"""Subpacote responsável pela extração de dados do Alpha Vantage."""

from __future__ import annotations

from .client import AlphaVantageClient
from .ingest_news import main as ingest_news

__all__ = ["AlphaVantageClient", "ingest_news"]
