import torch

from functools import lru_cache
from torch.utils.data import Dataset


LABELS = [
    "bills",
    "business",
    "debt_and_installment",
    "donation",
    "family_and_friends",
    "invest",
    "others",
    "shopping",
]


class CustomDataset(Dataset):
    def __init__(
        self,
        duckdb_conn,
        batch_size,
        data_split,
        tokenizer,
        text_encoder,
        labels=None,
        seed=42,
    ):
        self.conn = duckdb_conn
        self.batch_size = batch_size
        self.data_split = data_split
        self.tokenizer = tokenizer
        self.text_encoder = text_encoder
        self.labels = labels or LABELS
        self.labels_map = dict(zip(self.labels, range(len(self.labels))))
        self.seed = seed

        self._generate_batches()
        self.num_batches = self._get_num_batches()

    def __len__(self):
        return self.num_batches

    def _generate_labels(self):
        self.labels = (
            self.conn.sql("SELECT DISTINCT purpose FROM edges ORDER BY 1")
            .df()
            .purpose.tolist()
        )

    def _generate_batches(self):
        self.conn.execute(
            """
            CREATE OR REPLACE TEMPORARY TABLE batches AS

            -- Calculate the proportion of each purpose within the training set
            WITH purpose_proportions AS (
                SELECT
                    purpose,
                    pct_train AS proportion
                FROM
                    statistics_labels
            ),

            -- Number each row with respect to the purpose and data_split while ordering by hash to maintain randomness
            row_numbered AS (
                SELECT
                    e.data_split,
                    e.purpose,
                    e.trx_id,
                    ROW_NUMBER() OVER(
                        PARTITION BY e.data_split, e.purpose ORDER BY HASH(e.trx_id + $seed)
                    ) AS row_num1,
                    pp.proportion
                FROM
                    edges e
                    JOIN purpose_proportions pp ON e.purpose = pp.purpose
            ),

            -- Determine the batch size per purpose for each row
            batched AS (
                SELECT
                    data_split,
                    purpose,
                    trx_id,
                    row_num1,
                    proportion,
                    ROW_NUMBER() OVER(
                        PARTITION BY data_split ORDER BY row_num1 / proportion, purpose, trx_id
                    ) AS row_num2
                FROM
                    row_numbered
            ),

            -- Assign batch IDs based on the row numbers and batch size
            final AS (
                SELECT
                    *,
                    FLOOR(row_num2 / $batch_size)::INT AS batch_id
                FROM
                    batched
            )

            SELECT
                batch_id,
                data_split,
                trx_id,
                purpose
            FROM
                final
            """,
            {
                "batch_size": self.batch_size,
                "seed": self.seed,
            },
        )

    def _get_num_batches(self):
        return (
            self.conn.execute(
                f"""
            SELECT MAX(batch_id) + 1 FROM batches WHERE data_split = '{self.data_split}'
            """
            ).fetchone()[0]
            or 0
        )

    @lru_cache(maxsize=1024)
    def _get_labels(self, idx):
        df_labels = self.conn.execute(
            f"""
            WITH batch AS (
                SELECT
                    trx_id
                FROM
                    batches
                WHERE 1 = 1
                    AND data_split = '{self.data_split}'
                    AND batch_id = {idx}
            )

            SELECT
                e.purpose
            FROM
                batch AS b
            LEFT JOIN
                edges AS e USING(trx_id)
            ORDER BY
                b.trx_id
            """
        ).df()

        mapped = df_labels.purpose.map(self.labels_map).values
        tensor = torch.zeros((len(mapped), len(self.labels)), dtype=torch.float32)
        tensor[torch.arange(len(df_labels)), mapped] = 1

        return tensor

    @lru_cache(maxsize=1024)
    def _get_features(self, idx):
        df_features = self.conn.execute(
            f"""
            WITH batch AS (
                SELECT
                    trx_id
                FROM
                    batches
                WHERE 1 = 1
                    AND data_split = '{self.data_split}'
                    AND batch_id = {idx}
            )

            , features AS (
                SELECT
                    b.trx_id
                    , e.remark
                    , node_s.node_name AS sender_node_name
                    , node_b.node_name AS benef_node_name
                    , df.* EXCLUDE (calendar_date)
                    , amount_encoding(e.amount, 10_000, 100_000)         AS encoded_amount_10K_100K
                    , amount_encoding(e.amount, 100_000, 300_000)        AS encoded_amount_100K_300K
                    , amount_encoding(e.amount, 300_000, 1_000_000)      AS encoded_amount_300K_1M
                    , amount_encoding(e.amount, 1_000_000, 5_000_000)    AS encoded_amount_1M_5M
                    , amount_encoding(e.amount, 5_000_000, 10_000_000)   AS encoded_amount_5M_10M
                    , amount_encoding(e.amount, 10_000_000, 50_000_000)  AS encoded_amount_10M_50M
                    , amount_encoding(e.amount, 10_000, 50_000_000)      AS encoded_amount_10K_50M
                    , re.* EXCLUDE (remark)
                    , nne_s.* EXCLUDE (node_name)
                    , nne_b.* EXCLUDE (node_name)
                FROM
                    batch AS b
                LEFT JOIN
                    edges AS e USING(trx_id)
                LEFT JOIN
                    days_features AS df
                    ON e.trx_date = df.calendar_date
                LEFT JOIN
                    remark_embeddings AS re
                    ON e.remark = re.remark
                LEFT JOIN
                    nodes AS node_s
                    ON e.sender_node_id = node_s.id
                LEFT JOIN
                    nodes AS node_b
                    ON e.benef_node_id = node_b.id
                LEFT JOIN
                    node_name_embeddings AS nne_s
                    ON node_s.node_name = nne_s.node_name
                LEFT JOIN
                    node_name_embeddings AS nne_b
                    ON node_b.node_name = nne_b.node_name
            )

            SELECT
                *
            FROM
                features
            ORDER BY
                trx_id
            """
        ).df()

        # get df with no pre-computed embeddings
        for emb_col, col in zip(
            ["remark_emb_0", "node_name_emb_0", "node_name_emb_0_1"],
            ["remark", "sender_node_name", "benef_node_name"],
        ):
            mask_null = df_features[emb_col].isnull()
            df_no_emb = df_features.loc[mask_null, ["trx_id", col]]

            if df_no_emb.empty:
                continue

            embeddings = self.get_embeddings(df_no_emb[col].tolist())
            len_emb = embeddings.shape[1]
            splitted = emb_col.split("_")  # "{emb_col}_0_1" -> ["{emb_col}", "0", "1"]
            idx_emb = splitted.index("emb")
            emb_col_last = "_".join(splitted[: idx_emb + 1]) + f"_{len_emb - 1}"
            if len(splitted) > idx_emb + 2:
                emb_col_last += "_" + "_".join(splitted[idx_emb + 2 :])

            df_features.loc[mask_null, emb_col:emb_col_last] = embeddings.numpy()

        features = df_features.drop(
            columns=["trx_id", "remark", "sender_node_name", "benef_node_name"]
        )
        features = torch.tensor(features.values, dtype=torch.float32)
        return features

    @lru_cache(maxsize=1024)
    def __getitem__(self, idx):
        return self._get_features(idx), self._get_labels(idx)

    def get_embeddings(self, word_list):
        with torch.no_grad():
            embeddings = self.text_encoder(
                **self.tokenizer(
                    word_list,
                    padding=True,
                    truncation=True,
                    max_length=32,
                    return_tensors="pt",
                )
            ).last_hidden_state[:, 0, :]
        return embeddings
