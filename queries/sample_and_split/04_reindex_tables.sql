CREATE OR REPLACE TABLE sender_benef_pairs AS

SELECT
    ns.new_id::INT4 AS sender_node_id
    , nb.new_id::INT4 AS benef_node_id
    , sbp.* EXCLUDE(sender_node_id, benef_node_id)
FROM
    sender_benef_pairs AS sbp
LEFT JOIN
    nodes AS ns
ON
    sbp.sender_node_id = ns.id
LEFT JOIN
    nodes AS nb
ON
    sbp.benef_node_id = nb.id
ORDER BY
    1, 2

;

CREATE OR REPLACE TABLE edges AS

SELECT
    e.trx_id::INT4 AS trx_id
    , ns.new_id::INT4 AS sender_node_id
    , nb.new_id::INT4 AS benef_node_id
    , e.* EXCLUDE(trx_id, sender_node_id, benef_node_id)
FROM
    edges AS e
LEFT JOIN
    nodes AS ns
ON
    e.sender_node_id = ns.id
LEFT JOIN
    nodes AS nb
ON
    e.benef_node_id = nb.id

;

CREATE OR REPLACE TABLE nodes AS

SELECT new_id::INT4 AS id, * EXCLUDE(new_id, id) FROM nodes

;
