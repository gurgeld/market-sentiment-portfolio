WITH latest AS (
  SELECT
    ticker,
    MAX(published_date) AS last_date
  FROM gold.mart_daily_sentiment
  GROUP BY 1
)
SELECT
  d.ticker,
  d.published_date,
  d.article_count,
  d.avg_weighted_score,
  d.sentiment_index_100
FROM gold.mart_daily_sentiment d
JOIN latest l
  ON d.ticker = l.ticker
WHERE d.published_date >= l.last_date - INTERVAL 30 DAY;
