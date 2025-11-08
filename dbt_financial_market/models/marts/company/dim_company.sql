
{{ 
   config(  
            on_schema_change='fail',
            post_hook = create_index('id')
        )
}}

SELECT 
        map.company_id AS id,
        scp.symbol,
        scp.company_name,
        scp.cik,
        scp.isin,
        scp.cusip,
        scp.exchange_full_name,
        scp.exchange_code,
        scp.reported_currency,
        scp.sector,
        scp.industry,
        scp.country_code,
        scp.address_line,
        scp.city,
        scp.state_code,
        scp.postal_code,
        scp.phone_number,
        scp.ceo_name,
        scp.website_url,
        scp.logo_url,
        scp.full_time_employees,
        scp.is_actively_trading,
        scp.ipo_date,
        scp.is_etf,
        scp.is_adr,
        scp.is_fund,

        CURRENT_TIMESTAMP AS created_at
        
FROM {{ ref('stg_company_profile') }} AS scp
JOIN {{ ref('company_id_map') }}  AS map
 ON scp.symbol = map.symbol

