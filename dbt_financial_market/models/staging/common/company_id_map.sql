
--  persist the table, protect it from full refresh to stabilize ids

{% if flags.FULL_REFRESH %}

  {% do exceptions.raise_compiler_error("\n\n\t\t[ERROR]: Full refresh is not allowed for this model\n\n") %}

{% endif %}


{{  config( 
        materialized = 'incremental', 
        unique_key = 'symbol',
        on_schema_change = 'fail',
        contract={"enforced": True}
    ) 
}}


WITH all_symbols AS(
    SELECT DISTINCT symbol
    FROM {{ ref('stg_company_profile') }}
)


{% if not is_incremental() %} 

    {# init logic #}
    SELECT 
        (ROW_NUMBER() OVER(ORDER BY symbol))::integer AS company_id,
        symbol,
        CURRENT_TIMESTAMP AS created_at
    FROM all_symbols

{% else %}

    {# append logic #}
    SELECT  
        (ROW_NUMBER() OVER(ORDER BY symbol) + (SELECT max(company_id) FROM {{ this }} ))::integer AS company_id,
        symbol,
        CURRENT_TIMESTAMP AS created_at
    FROM all_symbols
    WHERE symbol NOT IN (SELECT symbol FROM {{ this }} )
    
{% endif %}