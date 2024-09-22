CREATE OR REPLACE TEMPORARY TABLE sender_benef_pairs_train AS

WITH edges_train AS (
    SELECT * FROM edges WHERE data_split = 'train'
)

, count_senders AS (
    SELECT
        sender_node_id
        , COUNT(DISTINCT trx_date) AS count_dates_sender
        , COUNT(DISTINCT trx_id) AS count_trx_sender
    FROM
        edges_train
    GROUP BY
        sender_node_id
)

, count_benefs AS (
    SELECT
        benef_node_id
        , COUNT(DISTINCT trx_date) AS count_dates_benef
        , COUNT(DISTINCT trx_id) AS count_trx_benef
    FROM
        edges_train
    GROUP BY
        benef_node_id
)

, pairs_train AS (
    SELECT
        e.sender_node_id
        , e.benef_node_id
        , COUNT(DISTINCT e.trx_date) AS count_dates
        , COUNT(DISTINCT e.trx_id) AS count_trx
        , MAX(cs.count_dates_sender) AS max_count_dates_sender
        , MAX(cb.count_dates_benef) AS max_count_dates_benef
        , MAX(cs.count_trx_sender) AS max_count_trx_sender
        , MAX(cb.count_trx_benef) AS max_count_trx_benef
        , MIN(e.trx_date) AS min_trx_date
        , MAX(e.trx_date) AS max_trx_date
        , SUM(count_trx) OVER(
            ORDER BY
                count_dates DESC
                , count_trx DESC
                , max_count_dates_sender DESC
                , max_count_dates_benef DESC
                , max_count_trx_sender DESC
                , max_count_trx_benef DESC
                , min_trx_date
                , max_trx_date DESC
                , e.sender_node_id
                , e.benef_node_id
        ) AS cumul_count_trx
    FROM
        edges_train AS e
    LEFT JOIN
        count_senders AS cs
    ON
        e.sender_node_id = cs.sender_node_id
    LEFT JOIN
        count_benefs AS cb
    ON
        e.benef_node_id = cb.benef_node_id
    GROUP BY
        e.sender_node_id
        , e.benef_node_id
)

SELECT
    sender_node_id
    , benef_node_id
    , count_trx
FROM
    pairs_train
WHERE 1 = 1
    {limit_edges_statement}
ORDER BY
    cumul_count_trx

;

CREATE OR REPLACE TEMPORARY TABLE sender_benef_pairs_valid AS

WITH edges_valid AS (
    SELECT * FROM edges WHERE data_split = 'valid'
)

, count_senders AS (
    SELECT
        sender_node_id
        , COUNT(DISTINCT trx_date) AS count_dates_sender
        , COUNT(DISTINCT trx_id) AS count_trx_sender
    FROM
        edges_valid
    GROUP BY
        sender_node_id
)

, count_benefs AS (
    SELECT
        benef_node_id
        , COUNT(DISTINCT trx_date) AS count_dates_benef
        , COUNT(DISTINCT trx_id) AS count_trx_benef
    FROM
        edges_valid
    GROUP BY
        benef_node_id
)

, pairs_valid AS (
    SELECT
        e.sender_node_id
        , e.benef_node_id
        , MAX(e.sender_node_id IN (SELECT sender_node_id FROM sender_benef_pairs_train)) AS is_sender_in_train
        , MAX(e.benef_node_id IN (SELECT benef_node_id FROM sender_benef_pairs_train)) AS is_benef_in_train
        , COUNT(DISTINCT e.trx_date) AS count_dates
        , COUNT(DISTINCT e.trx_id) AS count_trx
        , MAX(cs.count_dates_sender) AS max_count_dates_sender
        , MAX(cb.count_dates_benef) AS max_count_dates_benef
        , MAX(cs.count_trx_sender) AS max_count_trx_sender
        , MAX(cb.count_trx_benef) AS max_count_trx_benef
        , MIN(e.trx_date) AS min_trx_date
        , MAX(e.trx_date) AS max_trx_date
        , SUM(count_trx) OVER(
            ORDER BY
                (is_sender_in_train OR is_benef_in_train) DESC
                , is_sender_in_train DESC
                , is_benef_in_train DESC
                , count_dates DESC
                , count_trx DESC
                , max_count_dates_sender DESC
                , max_count_dates_benef DESC
                , max_count_trx_sender DESC
                , max_count_trx_benef DESC
                , min_trx_date
                , max_trx_date DESC
                , e.sender_node_id
                , e.benef_node_id
        ) AS cumul_count_trx
    FROM
        edges_valid AS e
    LEFT JOIN
        count_senders AS cs
    ON
        e.sender_node_id = cs.sender_node_id
    LEFT JOIN
        count_benefs AS cb
    ON
        e.benef_node_id = cb.benef_node_id
    WHERE 1 = 1
        AND (e.sender_node_id, e.benef_node_id) NOT IN (
            SELECT
                (sbpt.sender_node_id, sbpt.benef_node_id)
            FROM
                sender_benef_pairs_train AS sbpt
        )
    GROUP BY
        e.sender_node_id
        , e.benef_node_id
)

SELECT
    sender_node_id
    , benef_node_id
    , count_trx
FROM
    pairs_valid
WHERE 1 = 1
    {limit_edges_statement}
ORDER BY
    cumul_count_trx

;

CREATE OR REPLACE TEMPORARY TABLE sender_benef_pairs_test AS

WITH edges_test AS (
    SELECT * FROM edges WHERE data_split = 'test'
)

, count_senders AS (
    SELECT
        sender_node_id
        , COUNT(DISTINCT trx_date) AS count_dates_sender
        , COUNT(DISTINCT trx_id) AS count_trx_sender
    FROM
        edges_test
    GROUP BY
        sender_node_id
)

, count_benefs AS (
    SELECT
        benef_node_id
        , COUNT(DISTINCT trx_date) AS count_dates_benef
        , COUNT(DISTINCT trx_id) AS count_trx_benef
    FROM
        edges_test
    GROUP BY
        benef_node_id
)

, pairs_test AS (
    SELECT
        e.sender_node_id
        , e.benef_node_id
        , MAX(e.sender_node_id IN (SELECT sender_node_id FROM sender_benef_pairs_train)) AS is_sender_in_train
        , MAX(e.benef_node_id IN (SELECT benef_node_id FROM sender_benef_pairs_train)) AS is_benef_in_train
        , COUNT(DISTINCT e.trx_date) AS count_dates
        , COUNT(DISTINCT e.trx_id) AS count_trx
        , MAX(cs.count_dates_sender) AS max_count_dates_sender
        , MAX(cb.count_dates_benef) AS max_count_dates_benef
        , MAX(cs.count_trx_sender) AS max_count_trx_sender
        , MAX(cb.count_trx_benef) AS max_count_trx_benef
        , MIN(e.trx_date) AS min_trx_date
        , MAX(e.trx_date) AS max_trx_date
        , SUM(count_trx) OVER(
            ORDER BY
                (is_sender_in_train OR is_benef_in_train) DESC
                , is_sender_in_train DESC
                , is_benef_in_train DESC
                , count_dates DESC
                , count_trx DESC
                , max_count_dates_sender DESC
                , max_count_dates_benef DESC
                , max_count_trx_sender DESC
                , max_count_trx_benef DESC
                , min_trx_date
                , max_trx_date DESC
                , e.sender_node_id
                , e.benef_node_id
        ) AS cumul_count_trx
    FROM
        edges_test AS e
    LEFT JOIN
        count_senders AS cs
    ON
        e.sender_node_id = cs.sender_node_id
    LEFT JOIN
        count_benefs AS cb
    ON
        e.benef_node_id = cb.benef_node_id
    WHERE 1 = 1
        AND (e.sender_node_id, e.benef_node_id) NOT IN (
            SELECT
                (sbpt.sender_node_id, sbpt.benef_node_id)
            FROM
                sender_benef_pairs_train AS sbpt
        )
        AND (e.sender_node_id, e.benef_node_id) NOT IN (
            SELECT
                (sbpv.sender_node_id, sbpv.benef_node_id)
            FROM
                sender_benef_pairs_valid AS sbpv
        )
    GROUP BY
        e.sender_node_id
        , e.benef_node_id
)

SELECT
    sender_node_id
    , benef_node_id
    , count_trx
FROM
    pairs_test
WHERE 1 = 1
    {limit_edges_statement}
ORDER BY
    cumul_count_trx

;

CREATE OR REPLACE TABLE sender_benef_pairs AS

WITH pairs AS (
    SELECT *, 'train' AS data_split FROM sender_benef_pairs_train
    UNION ALL
    SELECT *, 'valid' AS data_split FROM sender_benef_pairs_valid
    UNION ALL
    SELECT *, 'test' AS data_split FROM sender_benef_pairs_test
)

SELECT
    sender_node_id
    , benef_node_id
    , MAX(IF(data_split = 'train', count_trx, 0))::INT4 AS count_trx_train
    , MAX(IF(data_split = 'valid', count_trx, 0))::INT4 AS count_trx_valid
    , MAX(IF(data_split = 'test', count_trx, 0))::INT4 AS count_trx_test
FROM
    pairs
GROUP BY
    sender_node_id
    , benef_node_id

;
