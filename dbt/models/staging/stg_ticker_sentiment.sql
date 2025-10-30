SELECT
  ts.news_id,
  UPPER(ts.ticker) AS ticker,
  CAST(ts.relevance_score AS DOUBLE) AS relevance_score,
  CAST(ts.ticker_sentiment_score AS DOUBLE) AS ticker_sentiment_score,
  ts.ticker_sentiment_label
FROM bronze.bronze_ticker_sentiment ts
JOIN silver.stg_news n ON n.id = ts.news_id