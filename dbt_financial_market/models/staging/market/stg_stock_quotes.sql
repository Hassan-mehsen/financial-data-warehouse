WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','stock_quotes') }}

),

renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                                               AS symbol,
       NULLIF( (elem->>'name'), '')::VARCHAR(200)                                                AS company_name,
       to_timestamp(NULLIF(elem->>'timestamp', '')::numeric)::date                               AS date,
       TO_CHAR(to_timestamp(NULLIF(elem->>'timestamp', '')::numeric)::date, 'YYYYMMDD')::integer AS date_key,
       NULLIF( (elem->>'price'), '')::numeric(20,2)                                              AS stock_price,
       NULLIF( (elem->>'changePercentage'), '')::numeric(10,6)                                   AS price_change_pct,
       NULLIF( (elem->>'change'), '')::numeric(10,6)                                             AS price_change,
       Round(NULLIF( (elem->>'volume'), '')::numeric, 0)::bigint                                 AS trading_volume,
       NULLIF( (elem->>'dayLow'), '')::numeric(10,2)                                             AS day_low,
       NULLIF( (elem->>'dayHigh'), '')::numeric(10,2)                                            AS day_high,
       NULLIF( (elem->>'yearHigh'), '')::numeric(10,2)                                           AS year_high,
       NULLIF( (elem->>'yearLow'), '')::numeric(10,2)                                            AS year_low,
       Round(NULLIF( (elem->>'marketCap'), '')::numeric ,0)::bigint                              AS market_cap,
       NULLIF( (elem->>'priceAvg50'), '')::numeric(12,4)                                         AS price_avg_50d,
       NULLIF( (elem->>'priceAvg200'), '')::numeric(12,4)                                        AS price_avg_200d,
       NULLIF( (elem->>'exchange'), '')::VARCHAR(100)                                            AS exchange_code,
       NULLIF( (elem->>'open'), '')::numeric(12,4)                                               AS open_price,
       NULLIF( (elem->>'previousClose'), '')::numeric(12,4)                                      AS last_close_price,
       source,
       ingestion_ts
    FROM extend_json
)

SELECT *
FROM renamed_and_typed
WHERE date_key IS NOT NULL AND symbol IS NOT NULL
