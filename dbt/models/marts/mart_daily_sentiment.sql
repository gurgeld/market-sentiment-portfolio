WITH per_article AS (
  SELECT
    s.ticker,
    n.published_date,
    -- peso por relevância do ticker no artigo
    s.ticker_sentiment_score * s.relevance_score AS weighted_score
  FROM silver.stg_ticker_sentiment s
  JOIN silver.stg_news n ON n.id = s.news_id
),
agg AS (
  SELECT
    ticker,
    published_date,
    COUNT(*) AS article_count,
    AVG(weighted_score) AS avg_weighted_score,
    AVG(CASE WHEN weighted_score > 0 THEN weighted_score END) AS avg_pos_score,
    AVG(CASE WHEN weighted_score < 0 THEN weighted_score END) AS avg_neg_score,
    SUM(CASE WHEN weighted_score > 0 THEN 1 ELSE 0 END)::INT AS pos_articles,
    SUM(CASE WHEN weighted_score < 0 THEN 1 ELSE 0 END)::INT AS neg_articles
  FROM per_article
  GROUP BY 1,2
),
indexing AS (
  SELECT
    *,
    -- índice 0–100 simples p/ visual
    50 + 50 * COALESCE(avg_weighted_score, 0) AS sentiment_index_100
  FROM agg
)
SELECT * FROM indexing;
