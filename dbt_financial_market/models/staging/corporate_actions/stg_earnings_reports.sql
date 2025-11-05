WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','earnings_reports') }}

),

renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                        AS symbol,
       NULLIF( (elem->>'date'), '')::date                                 AS date,
       TO_CHAR(NULLIF((elem->>'date'), '')::date, 'YYYYMMDD')::integer    AS date_key,
       NULLIF( (elem->>'epsActual'), '')::numeric(12,6)                   AS eps_actual,
       NULLIF( (elem->>'epsEstimated'), '')::numeric(12,6)                AS eps_estimated,
       NULLIF( (elem->>'revenueActual'), '')::numeric(20,2)               AS revenue_actual,
       NULLIF( (elem->>'revenueEstimated'), '')::numeric(20,2)            AS revenue_estimated,
       NULLIF( (elem->>'lastUpdated'), '')::date                          AS last_updated,
       source,
       ingestion_ts
    FROM extend_json
)

-- removing duplicates and keeping the most recent row 

SELECT DISTINCT ON (symbol, date_key,last_updated)  *
FROM renamed_and_typed
WHERE date_key IS NOT NULL AND symbol IS NOT NULL
ORDER BY symbol, date_key, last_updated DESC