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
    def __init__(self, duckdb_conn, batch_size, data_split, labels=None):
        self.conn = duckdb_conn
        self.batch_size = batch_size
        self.data_split = data_split

        self._generate_batches()
        self.num_batches = self._get_num_batches()
        self.labels = labels or LABELS

    def __len__(self):
        return self.num_batches

    def _generate_batches(self):
        self.conn.execute(
            """
            CREATE OR REPLACE TEMPORARY TABLE batches AS

            WITH batched AS (
                SELECT
                    trx_id
                    , data_split
                    , ROW_NUMBER() OVER (
                        PARTITION BY data_split ORDER BY trx_id
                    ) - 1 AS row_num,
                FROM
                    edges
            )

            SELECT
                *
                , FLOOR(row_num / $batch_size)::INT AS batch_id
            FROM
                batched
            """,
            {"batch_size": self.batch_size},
        )

    def _get_num_batches(self):
        return self.conn.execute(
            f"""
            SELECT MAX(batch_id) + 1 FROM batches WHERE data_split = '{self.data_split}'
            """
        ).fetchone()[0]

    @lru_cache(maxsize=2048)
    def _get_labels(self, idx):
        labels = self.conn.execute(
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

        labels_map = dict(zip(self.labels, range(len(LABELS))))
        tensor = torch.zeros(
            (len(labels.purpose.map(labels_map).values), len(self.labels)),
            dtype=torch.float32
        )
        tensor[torch.arange(len(labels)), labels.purpose.map(labels_map).values] = 1

        return tensor

    @lru_cache(maxsize=2048)
    def _get_features(self, idx):
        tensor = self.conn.execute(
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

            , stats_remark_features AS (
                SELECT
                    * EXCLUDE(feature_type)
                FROM
                    statistics_features
                WHERE 1 = 1
                    AND feature_type = 'remark'
            )

            , features AS (
                SELECT
                    b.trx_id
                    , df.* EXCLUDE (calendar_date)
                    , amount_encoding(e.amount, 10_000, 100_000)         AS encoded_amount_10K_100K
                    , amount_encoding(e.amount, 100_000, 300_000)        AS encoded_amount_100K_300K
                    , amount_encoding(e.amount, 300_000, 1_000_000)      AS encoded_amount_300K_1M
                    , amount_encoding(e.amount, 1_000_000, 5_000_000)    AS encoded_amount_1M_5M
                    , amount_encoding(e.amount, 5_000_000, 10_000_000)   AS encoded_amount_5M_10M
                    , amount_encoding(e.amount, 10_000_000, 50_000_000)  AS encoded_amount_10M_50M
                    , amount_encoding(e.amount, 10_000, 50_000_000)      AS encoded_amount_10K_50M
                    , IF(count_trx IS NULL,            0, count_trx / max_count_trx)                       AS count_trx
                    , IF(count_trx_not_others IS NULL, 0, count_trx_not_others / max_count_trx_not_others) AS count_trx_not_others
                    , IF(count_trx_bills IS NULL,      0, count_trx_bills / max_count_trx_bills)           AS count_trx_bills
                    , IF(count_trx_business IS NULL,   0, count_trx_business / max_count_trx_business)     AS count_trx_business
                    , IF(count_trx_debt IS NULL,       0, count_trx_debt / max_count_trx_debt)             AS count_trx_debt
                    , IF(count_trx_donation IS NULL,   0, count_trx_donation / max_count_trx_donation)     AS count_trx_donation
                    , IF(count_trx_family IS NULL,     0, count_trx_family / max_count_trx_family)         AS count_trx_family
                    , IF(count_trx_invest IS NULL,     0, count_trx_invest / max_count_trx_invest)         AS count_trx_invest
                    , IF(count_trx_shopping IS NULL,   0, count_trx_shopping / max_count_trx_shopping)     AS count_trx_shopping
                    , IF(count_trx_others IS NULL,     0, count_trx_others / max_count_trx_others)         AS count_trx_others
                    , IFNULL(rf.ratio_trx_bills, 0)                AS ratio_trx_bills
                    , IFNULL(rf.ratio_trx_business, 0)             AS ratio_trx_business
                    , IFNULL(rf.ratio_trx_debt, 0)                 AS ratio_trx_debt
                    , IFNULL(rf.ratio_trx_donation, 0)             AS ratio_trx_donation
                    , IFNULL(rf.ratio_trx_family, 0)               AS ratio_trx_family
                    , IFNULL(rf.ratio_trx_invest, 0)               AS ratio_trx_invest
                    , IFNULL(rf.ratio_trx_shopping, 0)             AS ratio_trx_shopping
                    , IFNULL(rf.ratio_trx_others, 0)               AS ratio_trx_others
                    , IFNULL(rf.ratio_trx_not_others, 0)           AS ratio_trx_not_others
                    , IFNULL(rf.ratio_trx_bills_non_others, 0)     AS ratio_trx_bills_non_others
                    , IFNULL(rf.ratio_trx_business_non_others, 0)  AS ratio_trx_business_non_others
                    , IFNULL(rf.ratio_trx_debt_non_others, 0)      AS ratio_trx_debt_non_others
                    , IFNULL(rf.ratio_trx_donation_non_others, 0)  AS ratio_trx_donation_non_others
                    , IFNULL(rf.ratio_trx_family_non_others, 0)    AS ratio_trx_family_non_others
                    , IFNULL(rf.ratio_trx_invest_non_others, 0)    AS ratio_trx_invest_non_others
                    , IFNULL(rf.ratio_trx_shopping_non_others, 0)  AS ratio_trx_shopping_non_others
                FROM
                    batch AS b
                LEFT JOIN
                    edges AS e USING(trx_id)
                LEFT JOIN
                    days_features AS df
                    ON e.trx_date = df.calendar_date
                LEFT JOIN
                    remark_features AS rf
                    ON e.remark = rf.remark
                CROSS JOIN
                    stats_remark_features AS srf
            )

            SELECT
                * EXCLUDE(trx_id)
            FROM
                features
            ORDER BY
                trx_id
            """
        ).df()

        tensor = torch.tensor(tensor.values, dtype=torch.float32)
        return tensor

    @lru_cache(maxsize=2048)
    def __getitem__(self, idx):
        return self._get_features(idx), self._get_labels(idx)
