"""Utilidades para IO com DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb

WAREHOUSE = Path("data/warehouse.duckdb")


def get_con() -> duckdb.DuckDBPyConnection:
    """Cria (se necessário) e retorna uma conexão com o warehouse local."""

    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(WAREHOUSE))


def ensure_schemas() -> None:
    """Garante que os schemas padrão existam no banco local."""

    con = get_con()
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.close()
