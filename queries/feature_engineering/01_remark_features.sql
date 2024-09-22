WITH count_trx AS (
    SELECT
        remark
        , COUNT(DISTINCT trx_id)::INT4 AS count_trx
        , COUNT(DISTINCT IF(purpose != 'others', trx_id, NULL))::INT4              AS count_trx_not_others
        , COUNT(DISTINCT IF(purpose = 'bills', trx_id, NULL))::INT4                AS count_trx_bills
        , COUNT(DISTINCT IF(purpose = 'business', trx_id, NULL))::INT4             AS count_trx_business
        , COUNT(DISTINCT IF(purpose = 'debt_and_installment', trx_id, NULL))::INT4 AS count_trx_debt
        , COUNT(DISTINCT IF(purpose = 'donation', trx_id, NULL))::INT4             AS count_trx_donation
        , COUNT(DISTINCT IF(purpose = 'family_and_friends', trx_id, NULL))::INT4   AS count_trx_family
        , COUNT(DISTINCT IF(purpose = 'invest', trx_id, NULL))::INT4               AS count_trx_invest
        , COUNT(DISTINCT IF(purpose = 'shopping', trx_id, NULL))::INT4             AS count_trx_shopping
        , COUNT(DISTINCT IF(purpose = 'others', trx_id, NULL))::INT4               AS count_trx_others
    FROM
        edges
    WHERE 1 = 1
        AND data_split = 'train'
    GROUP BY
        remark
)

SELECT
    *
    , IF(count_trx > 0, count_trx_bills / count_trx, 0)                          AS ratio_trx_bills
    , IF(count_trx > 0, count_trx_business / count_trx, 0)                       AS ratio_trx_business
    , IF(count_trx > 0, count_trx_debt / count_trx, 0)                           AS ratio_trx_debt
    , IF(count_trx > 0, count_trx_donation / count_trx, 0)                       AS ratio_trx_donation
    , IF(count_trx > 0, count_trx_family / count_trx, 0)                         AS ratio_trx_family
    , IF(count_trx > 0, count_trx_invest / count_trx, 0)                         AS ratio_trx_invest
    , IF(count_trx > 0, count_trx_shopping / count_trx, 0)                       AS ratio_trx_shopping
    , IF(count_trx > 0, count_trx_others / count_trx, 0)                         AS ratio_trx_others
    , IF(count_trx > 0, count_trx_not_others / count_trx, 0)                     AS ratio_trx_not_others
    , IF(count_trx_not_others > 0, count_trx_bills / count_trx_not_others, 0)    AS ratio_trx_bills_non_others
    , IF(count_trx_not_others > 0, count_trx_business / count_trx_not_others, 0) AS ratio_trx_business_non_others
    , IF(count_trx_not_others > 0, count_trx_debt / count_trx_not_others, 0)     AS ratio_trx_debt_non_others
    , IF(count_trx_not_others > 0, count_trx_donation / count_trx_not_others, 0) AS ratio_trx_donation_non_others
    , IF(count_trx_not_others > 0, count_trx_family / count_trx_not_others, 0)   AS ratio_trx_family_non_others
    , IF(count_trx_not_others > 0, count_trx_invest / count_trx_not_others, 0)   AS ratio_trx_invest_non_others
    , IF(count_trx_not_others > 0, count_trx_shopping / count_trx_not_others, 0) AS ratio_trx_shopping_non_others
FROM
    count_trx
ORDER BY
    remark

;
