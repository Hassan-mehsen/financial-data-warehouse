
{{ 
    config(
            incremental_strategy = 'append',
            on_schema_change='fail',
            post_hook = [
                create_index('id'),
                create_index('date_key')
            ]
        ) 
}}

WITH joined_market_snapshot AS (

    SELECT 
            ssq.symbol,
            ssq.date_key,
            ssf.free_float_pct,
            ssf.float_shares,
            ssf.outstanding_shares,
            ssq.stock_price,
            ssq.price_change_pct,
            ssq.price_change,
            ssq.trading_volume,
            ssq.day_low,
            ssq.day_high,
            ssq.year_high,
            ssq.year_low,
            ssq.market_cap,
            ssq.price_avg_50d,
            ssq.price_avg_200d,
            ssq.open_price,
            ssq.last_close_price,
            spc.change_pct_on_1d,
            spc.change_pct_on_5d,
            spc.change_pct_on_1m,
            spc.change_pct_on_3m,
            spc.change_pct_on_6m,
            spc.change_pct_ytd,
            spc.change_pct_on_1y,
            spc.change_pct_on_3y,
            spc.change_pct_on_5y,
            spc.change_pct_on_10y,
            spc.change_pct_max


    FROM {{ ref('stg_stock_quotes') }} AS ssq

    JOIN {{ ref('stg_price_changes') }} AS spc 
        ON  ssq.symbol = spc.symbol 
        AND ssq.date_key = spc.date_key

    JOIN {{ ref('stg_shares_float') }} AS ssf 
        ON  spc.symbol = ssf.symbol 
        AND spc.date_key = ssf.date_key
),

enriched_market AS (

    SELECT 
            jms.*,
            map.company_id AS id,
            CURRENT_TIMESTAMP AS created_at

    FROM joined_market_snapshot as jms
    JOIN {{ ref('company_id_map') }} AS map 
        ON jms.symbol = map.symbol
)


SELECT 
        id,
        date_key,
        stock_price,
        price_change,
        price_change_pct,
        trading_volume,
        day_low,
        day_high,
        year_high,
        year_low,
        market_cap,
        price_avg_50d,
        price_avg_200d,
        open_price,
        last_close_price,
        free_float_pct,
        float_shares,
        outstanding_shares,
        change_pct_on_1d,
        change_pct_on_5d,
        change_pct_on_1m,
        change_pct_on_3m,
        change_pct_on_6m,
        change_pct_ytd,
        change_pct_on_1y,
        change_pct_on_3y,
        change_pct_on_5y,
        change_pct_on_10y,
        change_pct_max,
        created_at

FROM enriched_market em
{% if is_incremental() %}

-- In other models, incremental logic is based on ingestion_ts.
-- Howerver, this model is built on 3 different sources(endpoints) with 3 different ingestion_ts, 
-- so using ingestion_ts is unreliable here
-- The current approach uses a composite key (id, date_key) to ensure data integrity at the cost of slightly reduced performance.
  
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS t
    WHERE t.id = em.id
      AND t.date_key = em.date_key
)

{% endif%}
 
        