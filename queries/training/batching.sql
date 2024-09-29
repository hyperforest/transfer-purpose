CREATE OR REPLACE TEMPORARY TABLE batches AS

WITH batched AS (
    SELECT
        trx_id
        , data_split
        , ROW_NUMBER() OVER (PARTITION BY data_split ORDER BY trx_id) - 1 AS row_num,
    FROM
        edges
)

SELECT
    *
    , FLOOR(row_num / $BATCH_SIZE)::INT AS batch_id
FROM
    batched

;
