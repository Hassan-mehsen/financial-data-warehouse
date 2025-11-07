
{{  config( 

        contract={"enforced": True}
    ) 
}}

WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','company_profile') }}

),

renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                      AS symbol,
       NULLIF( (elem->>'companyName'), '')::VARCHAR(200)                AS company_name,
       NULLIF( (elem->>'price'), '')::numeric(20,2)                     AS stock_price,
       Round(NULLIF( (elem->>'marketCap'), '')::numeric, 0)::bigint     AS market_cap,
       NULLIF( (elem->>'beta'), '')::numeric(10,6)                      AS beta_value,
       NULLIF( (elem->>'lastDividend'), '')::numeric(10,6)              AS last_dividend,
       NULLIF( (elem->>'range'), '')::VARCHAR(50)                       AS price_range,
       NULLIF( (elem->>'change'), '')::numeric(10,6)                    AS price_change,
       NULLIF( (elem->>'changePercentage'), '')::numeric(10,6)          AS price_change_pct,
       Round(NULLIF( (elem->>'volume'), '')::numeric, 0)::bigint        AS trading_volume,
       Round(NULLIF(elem->>'averageVolume', '')::numeric, 0)::bigint    AS avg_trading_volume,
       NULLIF( (elem->>'currency'), '')::VARCHAR(10)                    AS reported_currency, 
       NULLIF( (elem->>'cik'), '')::VARCHAR(50)                         AS cik,
       NULLIF( (elem->>'isin'), '')::VARCHAR(50)                        AS isin,
       NULLIF( (elem->>'cusip'), '')::VARCHAR(50)                       AS cusip,
       NULLIF( (elem->>'exchangeFullName'), '')::VARCHAR(200)           AS exchange_full_name,
       NULLIF( (elem->>'exchange'), '')::VARCHAR(100)                   AS exchange_code,
       NULLIF( (elem->>'industry'), '')::VARCHAR(200)                   AS industry,
       NULLIF( (elem->>'website'), '')::VARCHAR(200)                    AS website_url,
       NULLIF( (elem->>'description'), '')::TEXT                        AS company_description,
       NULLIF( (elem->>'ceo'), '')::VARCHAR(100)                        AS ceo_name,
       NULLIF( (elem->>'sector'), '')::VARCHAR(100)                     AS sector,
       NULLIF( (elem->>'country'), '')::VARCHAR(20)                     AS country_code,
       NULLIF(trim(elem->>'fullTimeEmployees'), '')::integer            AS full_time_employees,
       NULLIF( (elem->>'phone'), '')::VARCHAR(50)                       AS phone_number,
       NULLIF( (elem->>'address'), '')::VARCHAR(200)                    AS address_line,
       NULLIF( (elem->>'city'), '')::VARCHAR(50)                        AS city,
       NULLIF( (elem->>'state'), '')::VARCHAR(50)                       AS state_code,
       NULLIF( (elem->>'zip'), '')::VARCHAR(20)                         AS postal_code,
       NULLIF( (elem->>'image'), '')::VARCHAR(300)                      AS logo_url,
       NULLIF( (elem->>'ipoDate'), '')::date                            AS ipo_date,
       NULLIF( (elem->>'defaultImage'), '')::boolean                    AS has_default_logo,
       NULLIF( (elem->>'isEtf'), '')::boolean                           AS is_etf,
       NULLIF( (elem->>'isActivelyTrading'), '')::boolean               AS is_actively_trading,
       NULLIF( (elem->>'isAdr'), '')::boolean                           AS is_adr,
       NULLIF( (elem->>'isFund'), '')::boolean                          AS is_fund,

       source,
       ingestion_ts
    FROM extend_json
)

SELECT *
FROM renamed_and_typed
WHERE symbol IS NOT NULL
