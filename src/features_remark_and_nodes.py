import argparse
import duckdb
import sys

sys.path.append("./utils/")

from utils.time_utils import timeit  # type: ignore


@timeit
def create_remark_features(
    conn: duckdb.DuckDBPyConnection, dest_table: str | None = "remark_features"
) -> None:
    with open("./queries/feature_engineering/01_remark_features.sql") as f:
        query = f.read()

    if dest_table:
        query = f"CREATE OR REPLACE TABLE {dest_table} AS\n{query}"

    conn.execute(query)


@timeit
def create_node_trx_features(
    conn: duckdb.DuckDBPyConnection, dest_table: str | None = "node_trx_features"
) -> None:
    with open("./queries/feature_engineering/02_node_trx_features.sql") as f:
        query = f.read()

    if dest_table:
        query = f"CREATE OR REPLACE TABLE {dest_table} AS\n{query}"

    conn.execute(query)


@timeit
def create_days_features(
    conn: duckdb.DuckDBPyConnection, dest_table: str | None = "days_features"
) -> None:
    query = f"""
    CREATE OR REPLACE TABLE {dest_table} AS

    SELECT * FROM './datasets/raw/day_features.parquet'
    """

    conn.execute(query)


@timeit
def get_features_statistics(
    conn: duckdb.DuckDBPyConnection,
    dest_table: str | None = "statistics_features",
) -> None:
    with open("./queries/statistics/features_statistics.sql") as f:
        query = f.read()

    if dest_table:
        query = f"CREATE OR REPLACE TABLE {dest_table} AS\n{query}"

    conn.execute(query)


@timeit
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", type=str, required=True)
    args = parser.parse_args()

    conn = duckdb.connect(args.db_path)

    create_remark_features(conn, dest_table="remark_features")
    create_node_trx_features(conn, dest_table="node_trx_features")
    create_days_features(conn, dest_table="days_features")
    get_features_statistics(conn, dest_table="statistics_features")


if __name__ == "__main__":
    main()
