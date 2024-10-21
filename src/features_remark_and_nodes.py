import argparse
import duckdb
import sys
import warnings
import torch
import pandas as pd
from transformers import BertTokenizer, AutoModel

sys.path.append("./utils/")

from utils.time_utils import timeit  # type: ignore
from utils.db import load_db  # type: ignore

warnings.filterwarnings("ignore")

INDOBERT_MODEL = "indobert-lite-base-p2"


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


def generate_embeddings(
    conn: duckdb.DuckDBPyConnection,
    word_list: list[str],
    entity: str,
    tokenizer,
    text_encoder,
    batch_size=1000,
    table_name="embeddings",
):
    for i in range(0, len(word_list), batch_size):
        print(i, i + batch_size)
        word_list_batch = word_list[i : i + batch_size]
        tokenized = tokenizer(
            word_list_batch,
            padding=True,
            truncation=True,
            max_length=32,
            return_tensors="pt",
        )

        with torch.no_grad():
            outputs = text_encoder(**tokenized)

        embeddings = outputs.last_hidden_state[:, 0, :].numpy()
        df_emb = pd.DataFrame(embeddings)
        df_emb.insert(0, entity, word_list_batch)
        df_emb = df_emb.rename(
            columns={i: f"{entity}_emb_{i}" for i in range(len(df_emb.columns))}
        )

        if i == 0:
            conn.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_emb")
        else:
            conn.sql(f"INSERT INTO {table_name} SELECT * FROM df_emb")


@timeit
def create_remark_embeddings(
    conn: duckdb.DuckDBPyConnection,
    tokenizer,
    text_encoder,
    limit: int = 10000,
    dest_table: str | None = "remark_embeddings",
) -> None:
    df = conn.sql(
        """
        SELECT
            remark
            , COUNT(trx_id)::INT32 AS cnt
        FROM
            edges
        GROUP BY remark
        ORDER BY cnt DESC
        """
    ).df()
    df["pct"] = df["cnt"] / df["cnt"].sum()
    df["csum"] = df["pct"].cumsum()
    df = df.iloc[:limit]
    remark_list = df["remark"].tolist()

    generate_embeddings(
        conn=conn,
        word_list=remark_list,
        entity="remark",
        tokenizer=tokenizer,
        text_encoder=text_encoder,
        batch_size=1000,
        table_name=dest_table,
    )


@timeit
def create_node_name_embeddings(
    conn: duckdb.DuckDBPyConnection,
    tokenizer,
    text_encoder,
    limit: int = 10000,
    dest_table: str | None = "node_name_embeddings",
) -> None:
    df = conn.sql(
        """
        SELECT
            node_name
            , COUNT(id)::INT32 AS cnt
        FROM
            nodes
        GROUP BY node_name
        ORDER BY cnt DESC
        """
    ).df()
    df["pct"] = df["cnt"] / df["cnt"].sum()
    df["csum"] = df["pct"].cumsum()
    df = df.iloc[:limit]
    node_list = df["node_name"].tolist()

    generate_embeddings(
        conn=conn,
        word_list=node_list,
        entity="node_name",
        tokenizer=tokenizer,
        text_encoder=text_encoder,
        batch_size=1000,
        table_name=dest_table,
    )


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
    parser.add_argument("--limit_remark_embeddings", type=int, required=True)
    parser.add_argument("--limit_node_name_embeddings", type=int, required=True)
    args = parser.parse_args()

    tokenizer = BertTokenizer.from_pretrained(f"indobenchmark/{INDOBERT_MODEL}")
    indobert = AutoModel.from_pretrained(f"indobenchmark/{INDOBERT_MODEL}")

    conn = load_db(args.db_path)
    create_remark_features(conn, dest_table="remark_features")
    create_node_trx_features(conn, dest_table="node_trx_features")
    create_days_features(conn, dest_table="days_features")
    create_remark_embeddings(
        conn,
        tokenizer,
        indobert,
        dest_table="remark_embeddings",
        limit=args.limit_remark_embeddings,
    )
    create_node_name_embeddings(
        conn,
        tokenizer,
        indobert,
        dest_table="node_name_embeddings",
        limit=args.limit_node_name_embeddings,
    )
    get_features_statistics(conn, dest_table="statistics_features")


if __name__ == "__main__":
    main()
