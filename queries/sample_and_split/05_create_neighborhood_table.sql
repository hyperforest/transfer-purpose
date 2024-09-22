CREATE OR REPLACE TABLE neighborhood AS

WITH sender_benef AS (
    SELECT
        sender_node_id AS src_node_id
        , benef_node_id AS dst_node_id
        , count_trx_train
        , (
            count_trx_train
            + count_trx_valid
            + count_trx_test
        ) AS count_trx
    FROM
        sender_benef_pairs
)

, benef_sender AS (
    SELECT
        benef_node_id AS src_node_id
        , sender_node_id AS dst_node_id
        , count_trx_train
        , (
            count_trx_train
            + count_trx_valid
            + count_trx_test
        ) AS count_trx
    FROM
        sender_benef_pairs
)

, all_pairs AS (
    SELECT * FROM sender_benef
    UNION ALL
    SELECT * FROM benef_sender
)

, count_src AS (
    SELECT
        src_node_id
        , SUM(count_trx_train) AS count_trx_train
        , SUM(count_trx) AS count_trx
    FROM
        all_pairs
    GROUP BY
        src_node_id
)

, proportion AS (
    SELECT
        ap.*
        , IFNULL(ap.count_trx_train / cs.count_trx_train, 0) AS proportion_train
        , ap.count_trx / cs.count_trx AS proportion
    FROM
        all_pairs AS ap
    LEFT JOIN
        count_src AS cs
    USING(src_node_id)
)

SELECT
    src_node_id::INT4 AS src_node_id
    , dst_node_id::INT4 AS dst_node_id
    , count_trx_train::INT4 AS count_trx_train
    , count_trx::INT4 AS count_trx
    , proportion_train::FLOAT8 AS proportion_train
    , proportion::FLOAT8 AS proportion
FROM
    proportion
ORDER BY
    src_node_id
    , dst_node_id

;
