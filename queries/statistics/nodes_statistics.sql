CREATE OR REPLACE TABLE statistics_nodes AS

WITH senders AS (
    SELECT
        sender_node_id AS node_id
        , SUM(count_trx_train) AS count_trx_train
        , SUM(count_trx_valid) AS count_trx_valid
        , SUM(count_trx_test) AS count_trx_test
    FROM
        sender_benef_pairs
    GROUP BY
        sender_node_id
)

, benefs AS (
    SELECT
        benef_node_id AS node_id
        , SUM(count_trx_train) AS count_trx_train
        , SUM(count_trx_valid) AS count_trx_valid
        , SUM(count_trx_test) AS count_trx_test
    FROM
        sender_benef_pairs
    GROUP BY
        benef_node_id
)

, flags AS (
    SELECT
        node_id
        , (count_trx_train > 0) AS is_in_train
        , (count_trx_valid > 0) AS is_in_valid
        , (count_trx_test > 0) AS is_in_test
        , TRUE AS is_sender
    FROM
        senders
    UNION ALL
    SELECT
        node_id
        , (count_trx_train > 0) AS is_in_train
        , (count_trx_valid > 0) AS is_in_valid
        , (count_trx_test > 0) AS is_in_test
        , FALSE AS is_sender
    FROM
        benefs
)

SELECT
    is_sender
    , is_in_train
    , is_in_valid
    , is_in_test
    , COUNT(node_id) AS count_nodes
FROM
    flags
GROUP BY ALL
ORDER BY 1, 2, 3, 4

;
