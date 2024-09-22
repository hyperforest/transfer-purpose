WITH count_trx_sender AS (
    SELECT
        sender_node_id AS node_id
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
        sender_node_id
)

, sender_features AS (
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
        count_trx_sender
)

, count_trx_benef AS (
    SELECT
        benef_node_id AS node_id
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
        benef_node_id
)

, benef_features AS (
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
        count_trx_benef
)

, features AS (
    SELECT
        *
    FROM
        sender_features
    UNION ALL
    SELECT
        *
    FROM
        benef_features
)

, node_trx_features AS (
    SELECT
        nd.id AS node_id
        , nd.is_sender
        , IFNULL(ft.count_trx, 0)::INT4               AS count_trx
        , IFNULL(ft.count_trx_not_others, 0)::INT4    AS count_trx_not_others
        , IFNULL(ft.count_trx_bills, 0)::INT4         AS count_trx_bills
        , IFNULL(ft.count_trx_business, 0)::INT4      AS count_trx_business
        , IFNULL(ft.count_trx_debt, 0)::INT4          AS count_trx_debt
        , IFNULL(ft.count_trx_donation, 0)::INT4      AS count_trx_donation
        , IFNULL(ft.count_trx_family, 0)::INT4        AS count_trx_family
        , IFNULL(ft.count_trx_invest, 0)::INT4        AS count_trx_invest
        , IFNULL(ft.count_trx_shopping, 0)::INT4      AS count_trx_shopping
        , IFNULL(ft.count_trx_others, 0)::INT4        AS count_trx_others
        , IFNULL(ft.ratio_trx_bills, 0)               AS ratio_trx_bills
        , IFNULL(ft.ratio_trx_business, 0)            AS ratio_trx_business
        , IFNULL(ft.ratio_trx_debt, 0)                AS ratio_trx_debt
        , IFNULL(ft.ratio_trx_donation, 0)            AS ratio_trx_donation
        , IFNULL(ft.ratio_trx_family, 0)              AS ratio_trx_family
        , IFNULL(ft.ratio_trx_invest, 0)              AS ratio_trx_invest
        , IFNULL(ft.ratio_trx_shopping, 0)            AS ratio_trx_shopping
        , IFNULL(ft.ratio_trx_others, 0)              AS ratio_trx_others
        , IFNULL(ft.ratio_trx_not_others, 0)          AS ratio_trx_not_others
        , IFNULL(ft.ratio_trx_bills_non_others, 0)    AS ratio_trx_bills_non_others
        , IFNULL(ft.ratio_trx_business_non_others, 0) AS ratio_trx_business_non_others
        , IFNULL(ft.ratio_trx_debt_non_others, 0)     AS ratio_trx_debt_non_others
        , IFNULL(ft.ratio_trx_donation_non_others, 0) AS ratio_trx_donation_non_others
        , IFNULL(ft.ratio_trx_family_non_others, 0)   AS ratio_trx_family_non_others
        , IFNULL(ft.ratio_trx_invest_non_others, 0)   AS ratio_trx_invest_non_others
        , IFNULL(ft.ratio_trx_shopping_non_others, 0) AS ratio_trx_shopping_non_others
    FROM
        nodes AS nd
    LEFT JOIN
        features AS ft
    ON
        nd.id = ft.node_id
)

SELECT
    *
FROM
    node_trx_features
ORDER BY
    node_id

;
