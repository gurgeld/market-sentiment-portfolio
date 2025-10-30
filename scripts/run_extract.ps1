param(
  [string]$ApiKey = $env:ALPHAVANTAGE_API_KEY
)
if (-not $ApiKey) { Write-Error "Defina ALPHAVANTAGE_API_KEY no ambiente ou passe -ApiKey"; exit 1 }
$env:ALPHAVANTAGE_API_KEY = $ApiKey
python .\src\extract\ingest_news.py
