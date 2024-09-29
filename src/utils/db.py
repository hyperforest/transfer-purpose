import duckdb
from .features import amount_encoding

def load_db(db_path: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(db_path)
    conn.create_function("amount_encoding", amount_encoding, ["INT", "INT", "INT"], "FLOAT")
    return conn
