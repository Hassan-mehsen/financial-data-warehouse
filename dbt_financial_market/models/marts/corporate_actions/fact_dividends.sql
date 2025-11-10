 
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

WITH dividends_enriched AS(
    SELECT 

        map.company_id AS id,
        scd.date_key,

        scd.declaration_date,
        scd.record_date,
        scd.payment_date,
        
        scd.dividend,
        scd.yield,
        scd.adj_dividend,
        scd.frequency,

        scd.ingestion_ts,
        CURRENT_TIMESTAMP as created_at
        
    FROM {{ ref('stg_company_dividends') }} AS scd
    JOIN {{ ref('company_id_map') }} AS map
    ON scd.symbol = map.symbol
)

{% if is_incremental() %}

,last_ingestion AS (
    SELECT max(ingestion_ts) AS max_ts FROM {{ this }}
)

SELECT *
FROM dividends_enriched 
WHERE ingestion_ts > (SELECT max_ts FROM last_ingestion)

{% else %}

SELECT *
FROM dividends_enriched

{% endif%}