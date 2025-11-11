
{{ 
    config(
            incremental_strategy = 'append',
            on_schema_change='fail',
            post_hook = [
                create_index('id'),
                create_index('quarter')
            ]
        ) 
}}


WITH income_enriched AS (

    SELECT
        -- ID and time dimension
        map.company_id AS id,
        sis.date_key,
        sis.year,
        sis.quarter,

        sis.reported_currency,

        -- Revenue and cost
        sis.revenue,
        sis.cost_of_revenue,
        sis.cost_and_expenses,
        sis.gross_profit,

        -- Operating expenses
        sis.rd_expenses,
        sis.selling_marketing_expenses,
        sis.general_admin_expenses,
        sis.operating_expenses,
        sis.ebitda,
        sis.ebit,

        -- Non operating and financial items
        sis.interest_income,
        sis.interest_expense,
        sis.total_other_income_expenses_net,
        sis.income_before_tax,

        -- Net income and eps
        sis.net_income_continuing_ops,
        sis.net_income_discontinued_ops,
        sis.net_income,
        sis.net_income_deductions,
        sis.weighted_avg_shares_out,
        sis.weighted_avg_shares_out_diluted,
        sis.eps,
        sis.eps_diluted,

        -- Derived ratios
        ROUND(sis.gross_profit::numeric / NULLIF(sis.revenue,0)::numeric, 3)::numeric(8,3) AS gross_margin,
        ROUND(sis.ebit::numeric / NULLIF(sis.revenue,0)::numeric, 3)::numeric(8,3)         AS operating_margin,
        ROUND(sis.net_income::numeric / NULLIF(sis.revenue,0)::numeric, 3)::numeric(8,3)   AS net_margin,
        ROUND(sis.rd_expenses::numeric / NULLIF(sis.revenue,0)::numeric, 3)::numeric(8,3)  AS rd_to_revenue_ratio,

        -- metadata
        sis.ingestion_ts,
        CURRENT_TIMESTAMP AS created_at

    FROM {{ ref('stg_income_stmt') }} AS sis
    JOIN {{ ref('company_id_map') }} AS map
      ON sis.symbol = map.symbol
)

{% if is_incremental() %}

,last_ingestion AS (
    SELECT max(ingestion_ts) AS max_ts FROM {{ this }}
)

SELECT *
FROM income_enriched
WHERE ingestion_ts > (SELECT max_ts FROM last_ingestion)

{% else %}

SELECT *
FROM income_enriched

{% endif%}