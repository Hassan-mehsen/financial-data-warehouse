

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

WITH cashflow_enriched AS (

    SELECT
            -- ID and time dimension
            map.company_id AS id,
            scf.date_key,
            scf.year,
            scf.quarter,

            scf.reported_currency,

            -- Operating activities
            scf.net_income,
            scf.depreciation_and_amortization,
            scf.stock_based_compensation,
            scf.working_capital_changes,
            scf.net_cash_from_operating_activities,

            -- Investing activities
            scf.investments_in_property_plant_equipment,
            scf.net_acquisitions,
            scf.purchases_of_investments,
            scf.sales_maturities_of_investments,
            scf.net_cash_from_investing_activities,

            -- Finance activities
            scf.net_debt_issuance,
            scf.net_stock_issuance,
            scf.net_dividends_paid,
            scf.interest_paid,
            scf.other_financing_activities,
            scf.net_cash_from_financing_activities,

            -- Cash
            scf.effect_of_forex_changes_on_cash,
            scf.net_change_in_cash,
            scf.cash_at_beginning_of_period,
            scf.cash_at_end_of_period,
            scf.free_cash_flow,

            -- Derived ratios
            Round(scf.free_cash_flow::numeric / NULLIF(scf.net_income,0)::numeric, 2)::numeric(6,2)                     AS ratio_fcf_to_net_income,
            Round(scf.net_cash_from_operating_activities::numeric / NULLIF(scf.net_income,0)::numeric, 2)::numeric(6,2) AS ratio_operating_cf_to_net_income,
            ROUND(free_cash_flow::numeric / NULLIF(net_cash_from_operating_activities,0)::numeric, 2)::numeric(6,2)     AS ratio_fcf_to_operating_cf,

            -- metadata
            scf.ingestion_ts,
            CURRENT_TIMESTAMP AS created_at

    FROM {{ ref('stg_cash_flow_stmt') }} AS scf
    JOIN {{ ref('company_id_map') }} AS map 
      ON scf.symbol = map.symbol
)

{% if is_incremental() %}

,last_ingestion AS (
    SELECT max(ingestion_ts) AS max_ts FROM {{ this }}
)

SELECT *
FROM cashflow_enriched
WHERE ingestion_ts > (SELECT max_ts FROM last_ingestion)

{% else %}

SELECT *
FROM cashflow_enriched

{% endif%}