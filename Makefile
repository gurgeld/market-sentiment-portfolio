PY=python

setup:
	pip install -U pip
	pip install -r requirements.txt || pip install -e .

extract_news:
	$(PY) -m market_sentiment_portfolio.extract.ingest_news

transform:
	dbt deps --project-dir dbt
	dbt run --project-dir dbt --profiles-dir dbt
	dbt test --project-dir dbt --profiles-dir dbt

docs:
	dbt docs generate --project-dir dbt --profiles-dir dbt

app:
	streamlit run app/streamlit_app.py

all: extract_news transform docs

