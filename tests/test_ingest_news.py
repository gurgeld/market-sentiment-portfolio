"""Tests for the news ingestion helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from market_sentiment_portfolio.extract.ingest_news import iso8601


def test_iso8601_formats_timestamp_without_separators() -> None:
    ts = datetime(2024, 5, 1, 10, 30, 45, 123456, tzinfo=timezone.utc)

    assert iso8601(ts) == "20240501T103045"


def test_iso8601_assumes_naive_datetimes_are_utc() -> None:
    ts = datetime(2024, 5, 1, 10, 30, 45, 654321)

    assert iso8601(ts) == "20240501T103045"


def test_iso8601_normalizes_to_utc() -> None:
    ts = datetime(2024, 5, 1, 7, 30, 45, tzinfo=timezone(timedelta(hours=-3)))

    assert iso8601(ts) == "20240501T103045"
