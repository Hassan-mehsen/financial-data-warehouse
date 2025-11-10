{{ 
    config(
            incremental_strategy = 'append',
            on_schema_change = 'fail',
            post_hook = [
                create_index('id'),
                create_index('date_key')
            ]
        ) 
}} 

WITH earnings_enriched AS(
    SELECT 

        map.company_id AS id,
        ser.date_key,

        ser.eps_actual,
        ser.eps_estimated,
        -- when null then 0
        coalesce(ser.eps_actual - ser.eps_estimated, 0) AS eps_surprise,

        ser.revenue_actual,
        ser.revenue_estimated,
        -- when null then 0
        coalesce(ser.revenue_actual - ser.revenue_estimated,0) AS revenue_surprise,

        ser.last_updated,

        ser.ingestion_ts,
        CURRENT_TIMESTAMP as created_at
        
    FROM {{ ref('stg_earnings_reports') }} AS ser
    JOIN {{ ref('company_id_map') }} AS map
    ON ser.symbol = map.symbol
)

{% if is_incremental() %}

,last_ingestion AS (
    SELECT max(ingestion_ts) AS max_ts FROM {{ this }}
)

SELECT *
FROM earnings_enriched
WHERE ingestion_ts > (SELECT max_ts FROM last_ingestion)

{% else %}

SELECT *
FROM earnings_enriched

{% endif%}