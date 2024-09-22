WITH cte_node_trx_features AS (
    SELECT
        CASE
            WHEN is_sender = TRUE THEN 'sender_node'
            ELSE 'benef_node'
        END AS feature_type
        , MAX(count_trx)::INT4            AS max_count_trx
        , MAX(count_trx_not_others)::INT4 AS max_count_trx_not_others
        , MAX(count_trx_bills)::INT4      AS max_count_trx_bills
        , MAX(count_trx_business)::INT4   AS max_count_trx_business
        , MAX(count_trx_debt)::INT4       AS max_count_trx_debt
        , MAX(count_trx_donation)::INT4   AS max_count_trx_donation
        , MAX(count_trx_family)::INT4     AS max_count_trx_family
        , MAX(count_trx_invest)::INT4     AS max_count_trx_invest
        , MAX(count_trx_shopping)::INT4   AS max_count_trx_shopping
        , MAX(count_trx_others)::INT4     AS max_count_trx_others
    FROM
        node_trx_features
    GROUP BY
        is_sender
)

, cte_remark_features AS (
    SELECT
        'remark' AS feature_type
        , MAX(count_trx)::INT4            AS max_count_trx
        , MAX(count_trx_not_others)::INT4 AS max_count_trx_not_others
        , MAX(count_trx_bills)::INT4      AS max_count_trx_bills
        , MAX(count_trx_business)::INT4   AS max_count_trx_business
        , MAX(count_trx_debt)::INT4       AS max_count_trx_debt
        , MAX(count_trx_donation)::INT4   AS max_count_trx_donation
        , MAX(count_trx_family)::INT4     AS max_count_trx_family
        , MAX(count_trx_invest)::INT4     AS max_count_trx_invest
        , MAX(count_trx_shopping)::INT4   AS max_count_trx_shopping
        , MAX(count_trx_others)::INT4     AS max_count_trx_others
    FROM
        remark_features
)

, final AS (
    SELECT
        *
    FROM
        cte_node_trx_features
    UNION ALL
    SELECT
        *
    FROM
        cte_remark_features
)

SELECT * FROM final

;
