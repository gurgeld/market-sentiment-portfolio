param(
  [string]$ApiKey = $env:ALPHAVANTAGE_API_KEY
)
if (-not $ApiKey) { Write-Error "Defina ALPHAVANTAGE_API_KEY no ambiente ou passe -ApiKey"; exit 1 }
$env:ALPHAVANTAGE_API_KEY = $ApiKey
python -m market_sentiment_portfolio.extract.ingest_news

