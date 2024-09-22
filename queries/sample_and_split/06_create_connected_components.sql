CREATE OR REPLACE TEMPORARY TABLE connected_components AS

WITH RECURSIVE
    walks(node_id, front) AS (
        SELECT id AS node_id, id AS front
        FROM nodes
        UNION
        SELECT walks.node_id, neighborhood.dst_node_id AS front
        FROM walks, neighborhood
        WHERE walks.front = neighborhood.src_node_id
    ),

    components AS (
        SELECT node_id, MIN(front) AS component
        FROM walks
        GROUP BY node_id
    )

SELECT node_id, component AS connected_component_id
FROM components
ORDER BY component, node_id

;

CREATE OR REPLACE TABLE nodes AS

SELECT
    n.*
    , cc.connected_component_id
FROM
    nodes AS n
LEFT JOIN
    connected_components AS cc
ON
    n.id = cc.node_id

;
