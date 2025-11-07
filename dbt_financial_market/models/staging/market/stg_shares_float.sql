
{{  config( 

        contract={"enforced": True}
    ) 
}}

WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','shares_float') }}

),

renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                                AS symbol,
       NULLIF( (elem->>'date'), '')::timestamp::date                              AS date,
       TO_CHAR(NULLIF((elem->>'date'), '')::timestamp::date, 'YYYYMMDD')::integer AS date_key,
       NULLIF( (elem->>'freeFloat'), '' )::numeric(12,6)                          AS free_float_pct,  
       Round(NULLIF((elem->>'floatShares'), '')::numeric ,0)::bigint              AS float_shares,
       Round(NULLIF((elem->>'outstandingShares'), '')::numeric, 0)::bigint        AS outstanding_shares,
       source,
       ingestion_ts
    FROM extend_json
)

SELECT *
FROM renamed_and_typed
WHERE date_key IS NOT NULL AND symbol IS NOT NULL