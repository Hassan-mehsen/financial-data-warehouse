
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

WITH balance_enriched AS (

    SELECT 
            -- ID and time dimension
            map.company_id AS id,
            sbs.date_key,
            sbs.year,
            sbs.quarter,

            sbs.reported_currency,

            -- Cash and investemnts
            sbs.cash_and_cash_equivalents,
            sbs.short_term_investments,
            sbs.long_term_investments,
            sbs.total_investments,
            sbs.cash_and_short_term_investments,
            
            -- Current assets
            sbs.net_receivables,
            sbs.inventory,
            sbs.total_current_assets,
            
            -- Non current assets 
            sbs.property_plant_equipment_net,
            sbs.goodwill,
            sbs.intangible_assets,
            sbs.total_non_current_assets,
            sbs.total_assets,
            
            -- Cureent labilities
            sbs.account_payables,
            sbs.accrued_expenses,
            sbs.short_term_debt,
            sbs.total_current_liabilities,
           
            -- Non current labilities 
            sbs.long_term_debt,
            sbs.deferred_revenue_non_current,
            sbs.total_non_current_liabilities,
            sbs.total_liabilities,
           
            -- Equity
            sbs.common_stock,
            sbs.retained_earnings,
            sbs.additional_paid_in_capital,
            sbs.total_equity,
            sbs.total_liabilities_and_equity,
            
            -- Debt metrics
            sbs.total_debt,
            sbs.net_debt,
            
            -- Derived ratios
            Round( total_assets::numeric / NULLIF(sbs.total_liabilities,0)::numeric, 2)::numeric(6,2)   AS assets_to_liabilities_ratio,
            Round(total_debt::numeric / NULLIF(sbs.total_equity,0)::numeric, 2) ::numeric(6,2)          AS debt_to_equity_ratio,
            Round(total_current_assets::numeric / NULLIF(sbs.total_assets,0)::numeric, 2)::numeric(6,2) AS current_asset_ratio,
            
            -- metadata
            sbs.ingestion_ts,
            CURRENT_TIMESTAMP AS created_at

    FROM {{ ref('stg_balance_sheet_stmt') }} AS sbs
    JOIN {{ ref('company_id_map') }} AS map 
      ON sbs.symbol = map.symbol
)

{% if is_incremental() %}

,last_ingestion AS (
    SELECT max(ingestion_ts) AS max_ts FROM {{ this }}
)

SELECT *
FROM balance_enriched
WHERE ingestion_ts > (SELECT max_ts FROM last_ingestion)
{% else %}

SELECT *
FROM balance_enriched

{% endif%}
