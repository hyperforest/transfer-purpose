CREATE OR REPLACE TABLE sender_benef_pairs AS

WITH counted AS (
    SELECT
        sender_node_id
        , benef_node_id
        , COUNT(DISTINCT CASE WHEN data_split = 'train' THEN trx_id END)::INT AS count_trx_train
        , COUNT(DISTINCT CASE WHEN data_split = 'valid' THEN trx_id END)::INT AS count_trx_valid
        , COUNT(DISTINCT CASE WHEN data_split = 'test' THEN trx_id END)::INT AS count_trx_test
        , COUNT(DISTINCT trx_id)::INT AS count_trx_total
        , COUNT(DISTINCT CASE WHEN data_split = 'train' THEN purpose END)::INT AS count_labels_train
        , COUNT(DISTINCT CASE WHEN data_split = 'valid' THEN purpose END)::INT AS count_labels_valid
        , COUNT(DISTINCT CASE WHEN data_split = 'test' THEN purpose END)::INT AS count_labels_test
        , COUNT(DISTINCT CASE WHEN data_split = 'train' THEN trx_date END)::INT AS count_days_train
        , COUNT(DISTINCT CASE WHEN data_split = 'valid' THEN trx_date END)::INT AS count_days_valid
        , COUNT(DISTINCT CASE WHEN data_split = 'test' THEN trx_date END)::INT AS count_days_test
    FROM
        edges
    GROUP BY
        sender_node_id
        , benef_node_id
)

, filtered_and_ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER(
            ORDER BY
                count_trx_total DESC
                , count_trx_test DESC
                , count_trx_valid DESC
                , count_trx_train DESC
                , sender_node_id
                , benef_node_id
        ) AS pair_rank
    FROM
        counted
    WHERE 1 = 1
        AND count_trx_train > 0
        AND count_trx_valid > 0
        AND count_trx_test > 0
)

SELECT
    *
FROM
    filtered_and_ranked
{limit_edges_statement}

;
