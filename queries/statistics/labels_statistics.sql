CREATE OR REPLACE TABLE statistics_labels AS

WITH count_samples AS (
    SELECT
        purpose
        , COUNT(DISTINCT CASE WHEN data_split = 'train' THEN trx_id END)::INT AS cnt_train
        , COUNT(DISTINCT CASE WHEN data_split = 'valid' THEN trx_id END)::INT AS cnt_valid
        , COUNT(DISTINCT CASE WHEN data_split = 'test' THEN trx_id END)::INT AS cnt_test
    FROM
        edges
    GROUP BY
        purpose
)

SELECT
    *
    , (cnt_train / SUM(cnt_train) OVER())::FLOAT AS pct_train
    , (cnt_valid / SUM(cnt_valid) OVER())::FLOAT AS pct_valid
    , (cnt_test / SUM(cnt_test) OVER())::FLOAT AS pct_test
FROM
    count_samples
ORDER BY
    purpose

;
