 
 
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

WITH splits_enriched AS(
    SELECT 

        map.company_id AS id,
        sss.date_key,

        sss.numerator,
        sss.denominator,
        -- if the denominator is equal to 0, then ratio is null
        (sss.numerator / (NULLIF(sss.denominator,0)::float))::numeric(6,3) AS ratio, 

        sss.ingestion_ts,
        CURRENT_TIMESTAMP as created_at
        
    FROM {{ ref('stg_stock_splits') }} AS sss
    JOIN {{ ref('company_id_map') }} AS map
    ON sss.symbol = map.symbol
)

{% if is_incremental() %}

,last_ingestion AS (
    SELECT max(ingestion_ts) AS max_ts FROM {{ this }}
)

SELECT *
FROM splits_enriched
WHERE ingestion_ts > (SELECT max_ts FROM last_ingestion)

{% else %}

SELECT *
FROM splits_enriched

{% endif%}