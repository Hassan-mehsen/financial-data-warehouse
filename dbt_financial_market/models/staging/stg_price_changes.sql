WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','price_changes') }}

),

renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)          AS symbol,
       NULLIF( (elem->>'1D'), '')::numeric(16,6)            AS change_pct_on_1d,
       NULLIF( (elem->>'5D'), '' )::numeric(16,6)           AS change_pct_on_5d,  
       NULLIF( (elem->>'1M'), '' )::numeric(16,6)           AS change_pct_on_1m,
       NULLIF( (elem->>'3M'), '' )::numeric(16,6)           AS change_pct_on_3m,
       NULLIF( (elem->>'6M'), '' )::numeric(16,6)           AS change_pct_on_6m,
       NULLIF( (elem->>'ytd'), '' )::numeric(16,6)          AS change_pct_ytd,
       NULLIF( (elem->>'1Y'), '' )::numeric(16,6)           AS change_pct_on_1y,
       NULLIF( (elem->>'3Y'), '' )::numeric(16,6)           AS change_pct_on_3y,
       NULLIF( (elem->>'5Y'), '' )::numeric(16,6)           AS change_pct_on_5y,
       NULLIF( (elem->>'10Y'), '' )::numeric(16,6)          AS change_pct_on_10y,
       NULLIF( (elem->>'max'), '' )::numeric(16,6)          AS change_pct_max,
       source,
       ingestion_ts 
    FROM extend_json
)

SELECT *
FROM renamed_and_typed
WHERE symbol IS NOT NULL