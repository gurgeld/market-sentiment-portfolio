import os
import duckdb
import pandas as pd
import streamlit as st
import altair as alt
from datetime import date, timedelta

DB_PATH = "data/warehouse.duckdb"

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

def load_tickers(con):
    return [r[0] for r in con.execute("SELECT DISTINCT ticker FROM gold.mart_daily_sentiment ORDER BY 1").fetchall()]

st.set_page_config(page_title="Market News Sentiment", layout="wide")

st.title("📈 Market News Sentiment (Alpha Vantage)")

con = get_con()

# Filtros
all_tickers = load_tickers(con)
default = [t for t in ["AAPL","MSFT","GOOGL","AMZN","XOM","PG"] if t in all_tickers] or all_tickers[:5]
sel_tickers = st.multiselect("Tickers", options=all_tickers, default=default)

end = date.today()
start = end - timedelta(days=90)
d1, d2 = st.date_input("Período", (start, end))

if sel_tickers:
    q = """
    SELECT *
    FROM gold.mart_daily_sentiment
    WHERE ticker IN ({tickers})
      AND published_date BETWEEN ? AND ?
    ORDER BY published_date
    """.format(tickers=",".join(["?"]*len(sel_tickers)))
    df = con.execute(q, [*sel_tickers, pd.Timestamp(d1), pd.Timestamp(d2)]).df()

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    if not df.empty:
        latest = df.sort_values(["ticker","published_date"]).groupby("ticker").tail(1)
        col1.metric("Tickers", len(sel_tickers))
        col2.metric("Artigos (janela)", int(df["article_count"].sum()))
        col3.metric("Sentimento médio (últ. dia)", f"{latest['avg_weighted_score'].mean():.3f}")
        col4.metric("Índice (0–100)", f"{latest['sentiment_index_100'].mean():.1f}")

        # Série temporal
        line = alt.Chart(df).mark_line().encode(
            x="published_date:T",
            y=alt.Y("sentiment_index_100:Q", title="Sentiment Index (0–100)"),
            color="ticker:N",
            tooltip=["ticker","published_date","article_count","avg_weighted_score","sentiment_index_100"]
        ).properties(height=360)
        st.altair_chart(line, use_container_width=True)

        # Heatmap últimos 30 dias por ticker
        heat = con.execute("""
            SELECT ticker, published_date, sentiment_index_100
            FROM gold.mart_ticker_heatmap
            WHERE ticker IN ({})
        """.format(",".join(["?"]*len(sel_tickers))), sel_tickers).df()
        if not heat.empty:
            pivot = heat.pivot_table(index="ticker", columns="published_date", values="sentiment_index_100")
            st.dataframe(pivot, use_container_width=True)
    else:
        st.info("Sem dados para os filtros escolhidos.")
else:
    st.warning("Selecione pelo menos um ticker.")
