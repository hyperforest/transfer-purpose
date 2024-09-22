CREATE OR REPLACE TABLE nodes AS
    
SELECT * FROM '{nodes_path}'

;

CREATE OR REPLACE TABLE edges AS
    
SELECT
    *
    , CASE
        WHEN trx_date >= $date_train_start AND trx_date <= $date_train_end THEN 'train'
        WHEN trx_date >= $date_valid_start AND trx_date <= $date_valid_end THEN 'valid'
        WHEN trx_date >= $date_test_start  AND trx_date <= $date_test_end  THEN 'test'
    END AS data_split
FROM
    '{edges_path}'

;
