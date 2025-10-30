# market-sentiment-portfolio
Alpha Vantage Market Sentiment Data Pipeline

Package Requirements: 
    - requests
    - duckdb
    - pandas
    - pyarrow
    - python-dateutil
    - tqdm
    - dbt-duckdb
    - streamlit
    - altair



Inclua no README:

Diagrama simples: Alpha Vantage → Ingestão (Python) → DuckDB → dbt (staging/marts) → Streamlit

Prints do Lineage graph do dbt

Limites da API e como você trata rate limiting e idempotência

Próximos passos (abaixo)