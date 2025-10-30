import os
import duckdb
from pathlib import Path

WAREHOUSE = Path("data/warehouse.duckdb")

def get_con():
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE))
    return con

def ensure_schemas():
    con = get_con()
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.close()