WITH base AS (
  SELECT
    id,
    title,
    summary,
    source,
    url,
    time_published,
    CAST(overall_sentiment_score AS DOUBLE) AS overall_sentiment_score,
    overall_sentiment_label,
    topics,
    ingested_at
  FROM bronze.bronze_news
),
parsed AS (
  SELECT
    id,
    title,
    summary,
    source,
    url,
    -- "20250122T153200" → timestamp
    CAST(
      STRPTIME(time_published, '%Y%m%dT%H%M%S')
      AS TIMESTAMP
    ) AT TIME ZONE 'UTC' AS published_ts_utc,
    DATE_TRUNC('day', CAST(STRPTIME(time_published, '%Y%m%dT%H%M%S') AS TIMESTAMP)) AS published_date,
    overall_sentiment_score,
    overall_sentiment_label,
    topics,
    ingested_at
  FROM base
)
SELECT * FROM parsed
