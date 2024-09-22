CREATE OR REPLACE TABLE nodes AS

WITH new_nodes AS (
    SELECT DISTINCT sender_node_id AS node_id FROM sender_benef_pairs
    UNION ALL
    SELECT DISTINCT benef_node_id AS node_id FROM sender_benef_pairs
)

SELECT
    n.*
    , ROW_NUMBER() OVER(ORDER BY n.id) AS new_id
FROM
    nodes AS n
JOIN
    new_nodes AS nn
ON
    n.id = nn.node_id

;

CREATE OR REPLACE TABLE edges AS

SELECT DISTINCT
    e.*
FROM
    edges AS e
JOIN
    sender_benef_pairs AS sbp
USING(
    sender_node_id
    , benef_node_id
)
WHERE 1 = 1
    AND (
        (e.data_split = 'train' AND sbp.count_trx_train > 0)
        OR (e.data_split = 'valid' AND sbp.count_trx_valid > 0)
        OR (e.data_split = 'test' AND sbp.count_trx_test > 0)
    )
ORDER BY
    trx_date
    , trx_id

;
