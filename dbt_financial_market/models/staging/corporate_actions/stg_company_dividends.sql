WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','company_dividends') }}

),

renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                       AS symbol,
       NULLIF( (elem->>'date'), '')::date                                AS date,
       TO_CHAR(NULLIF((elem->>'date'), '')::date, 'YYYYMMDD')::integer   AS date_key,
       NULLIF( (elem->>'recordDate'), '')::date                          AS record_date,
       NULLIF( (elem->>'paymentDate'), '')::date                         AS payment_date,
       NULLIF( (elem->>'declarationDate'), '')::date                     AS declaration_date,
       NULLIF( (elem->>'adjDividend'), '')::numeric(10,8)                AS adj_dividend,
       NULLIF( (elem->>'dividend'), '')::numeric(10,8)                   AS dividend,
       NULLIF( (elem->>'yield'), '')::numeric(12,8)                      AS yield,
       NULLIF( (elem->>'frequency'), '')::VARCHAR(50)                    AS frequency,
       source,
       ingestion_ts
    FROM extend_json
)

SELECT *
FROM renamed_and_typed
WHERE date_key IS NOT NULL AND symbol IS NOT NULL
