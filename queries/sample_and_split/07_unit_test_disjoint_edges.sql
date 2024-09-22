WITH flags AS (
    SELECT
        IF(count_trx_train > 0, (count_trx_valid = 0) AND (count_trx_test = 0), NULL) AS is_train_only
        , IF(count_trx_valid > 0, (count_trx_train = 0) AND (count_trx_test = 0), NULL) AS is_valid_only
        , IF(count_trx_test > 0, (count_trx_train = 0) AND (count_trx_valid = 0), NULL) AS is_test_only
    FROM
        sender_benef_pairs
)

SELECT
    (
        MAX(is_train_only) = TRUE
        AND MAX(is_valid_only) = TRUE
        AND MAX(is_test_only) = TRUE
    )
    AS is_disjoint
FROM
    flags

;
