CREATE OR REPLACE TABLE statistics AS

WITH nodes_stat AS (
    SELECT
        'count_senders' AS stat
        , COUNT(DISTINCT id)::INT4 AS number
    FROM
        nodes
    WHERE 1 = 1
        AND is_sender = TRUE
    UNION ALL
    SELECT
        'count_benefs' AS stat
        , COUNT(DISTINCT id)::INT4 AS number
    FROM
        nodes
    WHERE 1 = 1
        AND is_sender = FALSE
    UNION ALL
    SELECT
        'count_sender_benef_pairs' AS stat
        , COUNT(DISTINCT (sender_node_id, benef_node_id))::INT4 AS number
    FROM
        sender_benef_pairs
    UNION ALL
    SELECT
        'count_unique_edges' AS stat
        , COUNT(DISTINCT (src_node_id, dst_node_id))::INT4 AS number
    FROM
        neighborhood
    ORDER BY
        stat
)

, edges_stat AS (
    SELECT
        'edges_' || data_split AS stat
        , COUNT(DISTINCT trx_id)::INT4 AS number
    FROM
        edges
    GROUP BY
        stat
    ORDER BY
        stat
)

, all_stat AS (
    SELECT
        'nodes' AS stat
        , SUM(number)::INT4 AS number
    FROM
        nodes_stat
    UNION ALL
    SELECT
        'edges' AS stat
        , SUM(number)::INT4 AS number
    FROM
        edges_stat
    ORDER BY
        stat DESC
)

SELECT * FROM all_stat
UNION ALL
SELECT * FROM nodes_stat
UNION ALL
SELECT * FROM edges_stat

;
