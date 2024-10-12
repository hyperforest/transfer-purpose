import os
import argparse
import duckdb
import sys

sys.path.append("./utils/")

from utils.time_utils import timeit  # type: ignore


@timeit
def create_nodes_and_edges_table(
    conn: duckdb.DuckDBPyConnection, nodes_path: str, edges_path: str, date_params: dict
) -> None:
    with open("./queries/sample_and_split/01_create_nodes_and_edges_table.sql") as f:
        query = f.read()

    conn.execute(
        query.format(nodes_path=nodes_path, edges_path=edges_path), date_params
    )


@timeit
def create_sender_benef_pairs_table(
    conn: duckdb.DuckDBPyConnection, limit_edges: int | None
) -> None:
    with open("./queries/sample_and_split/02_create_sender_benef_pairs_table.sql") as f:
        query = f.read()

    limit_edges_statement = (
        f"QUALIFY SUM(count_trx_total) OVER(ORDER BY pair_rank) <= {limit_edges}"
        if limit_edges
        else ""
    )

    conn.execute(query.format(limit_edges_statement=limit_edges_statement))


@timeit
def resample_nodes_and_edges_table(conn: duckdb.DuckDBPyConnection) -> None:
    with open("./queries/sample_and_split/03_resample_nodes_and_edges.sql") as f:
        query = f.read()

    conn.execute(query)


@timeit
def reindex_tables(conn: duckdb.DuckDBPyConnection) -> None:
    with open("./queries/sample_and_split/04_reindex_tables.sql") as f:
        query = f.read()

    conn.execute(query)


@timeit
def create_neighborhood_table(conn: duckdb.DuckDBPyConnection) -> None:
    with open("./queries/sample_and_split/05_create_neighborhood_table.sql") as f:
        query = f.read()

    conn.execute(query)


@timeit
def create_connected_components(conn: duckdb.DuckDBPyConnection) -> None:
    with open("./queries/sample_and_split/06_create_connected_components.sql") as f:
        query = f.read()

    conn.execute(query)


@timeit
def get_general_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    with open("./queries/statistics/general_statistics.sql") as f:
        query = f.read()

    conn.sql(query)


@timeit
def get_nodes_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    with open("./queries/statistics/nodes_statistics.sql") as f:
        query = f.read()

    conn.execute(query)


@timeit
def get_labels_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    with open("./queries/statistics/labels_statistics.sql") as f:
        query = f.read()

    conn.execute(query)


@timeit
def get_connected_component_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    with open("./queries/statistics/connected_components_statistics.sql") as f:
        query = f.read()

    conn.execute(query)


@timeit
def show_statistics(conn: duckdb.DuckDBPyConnection, count_cc_stat=False) -> None:
    stat_tables = ["statistics", "statistics_nodes"]
    if count_cc_stat:
        stat_tables.append("statistics_connected_components")

    for table in stat_tables:
        print(f"--- {table} ---")
        print(conn.sql(f"SELECT * FROM {table}"))


@timeit
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--nodes_path", type=str, required=True)
    parser.add_argument("--edges_path", type=str, required=True)
    parser.add_argument("--db_path", type=str, required=True)
    parser.add_argument("--date_train_start", type=str, default="2024-05-01")
    parser.add_argument("--date_train_end", type=str, default="2024-05-31")
    parser.add_argument("--date_valid_start", type=str, default="2024-06-01")
    parser.add_argument("--date_valid_end", type=str, default="2024-06-30")
    parser.add_argument("--date_test_start", type=str, default="2024-07-01")
    parser.add_argument("--date_test_end", type=str, default="2024-07-31")
    parser.add_argument("--limit_edges", type=int, default=None)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    # if db_path exist, remove it
    if os.path.exists(args.db_path):
        os.remove(args.db_path)

    date_params = {
        "date_train_start": args.date_train_start,
        "date_train_end": args.date_train_end,
        "date_valid_start": args.date_valid_start,
        "date_valid_end": args.date_valid_end,
        "date_test_start": args.date_test_start,
        "date_test_end": args.date_test_end,
    }

    conn = duckdb.connect(args.db_path)

    create_nodes_and_edges_table(
        conn,
        nodes_path=args.nodes_path,
        edges_path=args.edges_path,
        date_params=date_params,
    )
    create_sender_benef_pairs_table(conn, limit_edges=args.limit_edges)
    resample_nodes_and_edges_table(conn)
    reindex_tables(conn)
    create_neighborhood_table(conn)

    count_unique_edges = conn.sql(
        "SELECT COUNT(DISTINCT (src_node_id, dst_node_id)) FROM neighborhood"
    ).fetchall()[0][0]

    is_feasible_to_create_cc = count_unique_edges <= 100000

    if is_feasible_to_create_cc:
        create_connected_components(conn)

    get_general_statistics(conn)
    get_nodes_statistics(conn)
    get_labels_statistics(conn)

    if is_feasible_to_create_cc:
        get_connected_component_statistics(conn)

    conn.close()


if __name__ == "__main__":
    main()
