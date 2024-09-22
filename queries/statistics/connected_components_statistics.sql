CREATE OR REPLACE TABLE statistics_connected_components AS

WITH components_count AS (
    SELECT
        connected_component_id
        , COUNT(DISTINCT node_id) AS count_nodes
    FROM
        connected_components
    GROUP BY
        connected_component_id
)

, components_size AS (
    SELECT
        'size_cc' AS stat
        , connected_component_id
        , count_nodes
        , ROW_NUMBER() OVER (ORDER BY count_nodes DESC) AS rnk
    FROM
        components_count
)

SELECT
    'count_cc' AS stat
    , COUNT(connected_component_id) AS number
FROM
    components_count
UNION ALL
SELECT
    'max_size_cc_1st' AS stat
    , count_nodes AS number
FROM
    components_size
WHERE 1 = 1
    AND rnk = 1
UNION ALL
SELECT
    'max_size_cc_2nd' AS stat
    , count_nodes AS number
FROM
    components_size
WHERE 1 = 1
    AND rnk = 2
ORDER BY stat

;
