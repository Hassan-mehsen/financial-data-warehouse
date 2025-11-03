WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','stock_splits') }}

),

renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                     AS symbol,
       NULLIF( (elem->>'date'), '')::date                              AS date,
       TO_CHAR(NULLIF((elem->>'date'), '')::date, 'YYYYMMDD')::integer AS date_key,
       NULLIF( (elem->>'numerator'), '' )::integer                     AS numerator,  
       NULLIF( (elem->>'denominator'), '' )::integer                   AS denominator,
       source,
       ingestion_ts
    FROM extend_json
)

SELECT *
FROM renamed_and_typed
WHERE date_key IS NOT NULL AND symbol IS NOT NULL