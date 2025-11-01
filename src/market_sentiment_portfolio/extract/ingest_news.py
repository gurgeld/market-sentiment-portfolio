"""Pipeline para ingerir notícias do Alpha Vantage."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd

from market_sentiment_portfolio.extract.client import AlphaVantageClient
from market_sentiment_portfolio.utils.io import ensure_schemas, get_con

# Config – ajuste tickers e tópicos do seu MVP
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "XOM", "PG"]
TOPICS  = ["technology"]  # veja doc oficial p/ lista
# janela de coleta (últimos 7 dias)
DAYS_BACK = int(os.getenv("NEWS_DAYS_BACK", "7"))

def iso8601(ts: datetime) -> str:
    """Normaliza timestamps para o formato aceito pela API."""

    # A API do Alpha Vantage exige o formato compacto `YYYYMMDDTHHMMSS`, sem
    # separadores ou deslocamentos como ``+00:00``. ``datetime.isoformat`` gera
    # strings como ``2024-05-01T10:30:00+00:00`` que são recusadas pelo
    # endpoint, portanto formatamos manualmente e garantimos normalização para
    # UTC.
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc).replace(microsecond=0)
    return ts.strftime("%Y%m%dT%H%M%S")

def main() -> None:
    ensure_schemas()
    client = AlphaVantageClient()

    time_to   = datetime.now(timezone.utc)
    time_from = time_to - timedelta(days=DAYS_BACK)

    params = {
        "function": "NEWS_SENTIMENT",
        # você pode usar 'tickers' OU 'topics' — combine com parcimônia
        #"tickers": ",".join(TICKERS),
        "topics": ",".join(TOPICS),
        "sort": "LATEST",
        "time_from": iso8601(time_from),
        "time_to": iso8601(time_to),
        "limit": 1000,  # máximo por chamada
    }
    data = client.call(**params)

    feed = data.get("feed", [])
    if not feed:
        print("Sem notícias retornadas.")
        return

    # Normalização: duas tabelas bronze
    # 1) notícias (uma linha por artigo)
    news_rows = []
    ts_ingest = datetime.utcnow()
    for item in feed:
        news_rows.append({
            "id":            item.get("url"),   # chave estável = url
            "title":         item.get("title"),
            "summary":       item.get("summary"),
            "source":        item.get("source"),
            "url":           item.get("url"),
            "time_published": item.get("time_published"),  # "20250122T153200"
            "overall_sentiment_score": item.get("overall_sentiment_score"),
            "overall_sentiment_label": item.get("overall_sentiment_label"),
            "topics":        str(item.get("topics", [])),
            "raw_json":      item,  # guarda tudo
            "ingested_at":   ts_ingest,
        })

    news_df = pd.DataFrame(news_rows)

    # 2) ticker_sentiment (uma linha por artigo×ticker)
    ts_rows = []
    for item in feed:
        url = item.get("url")
        for ts in item.get("ticker_sentiment", []):
            ts_rows.append({
                "news_id": url,
                "ticker": ts.get("ticker"),
                "relevance_score": float(ts.get("relevance_score", 0) or 0),
                "ticker_sentiment_score": float(ts.get("ticker_sentiment_score", 0) or 0),
                "ticker_sentiment_label": ts.get("ticker_sentiment_label"),
            })
    ts_df = pd.DataFrame(ts_rows)

    con = get_con()
    try:
        # UPSERT simples por chave
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.bronze_news (
                id TEXT PRIMARY KEY,
                title TEXT,
                summary TEXT,
                source TEXT,
                url TEXT,
                time_published TEXT,
                overall_sentiment_score DOUBLE,
                overall_sentiment_label TEXT,
                topics TEXT,
                raw_json JSON,
                ingested_at TIMESTAMP
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.bronze_ticker_sentiment (
                news_id TEXT,
                ticker TEXT,
                relevance_score DOUBLE,
                ticker_sentiment_score DOUBLE,
                ticker_sentiment_label TEXT
            );
            """
        )

        # upsert news
        con.register("news_df", news_df)
        con.execute(
            """
            DELETE FROM bronze.bronze_news
            WHERE id IN (SELECT id FROM news_df);
            """
        )
        con.execute(
            """
            INSERT INTO bronze.bronze_news
            SELECT * FROM news_df;
            """
        )

        # replace ticker_sentiment do período (simples)
        con.execute(
            "DELETE FROM bronze.bronze_ticker_sentiment WHERE news_id IN (SELECT id FROM bronze.bronze_news);"
        )
        con.register("ts_df", ts_df)
        con.execute(
            """
            INSERT INTO bronze.bronze_ticker_sentiment
            SELECT * FROM ts_df;
            """
        )
    finally:
        con.close()
    print(f"Inseridos {len(news_df)} artigos e {len(ts_df)} linhas de ticker_sentiment.")

if __name__ == "__main__":
    main()
