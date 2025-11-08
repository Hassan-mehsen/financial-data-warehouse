
{{  config( 

        contract={"enforced": True}
    ) 
}}

WITH typed as (

  SELECT

    date_key::INTEGER       AS date_key,
    date::DATE              AS date,
    month::INTEGER          AS month,
    year::INTEGER           AS year,
    fiscal_quarter::INTEGER AS quarter,
    day_of_week::INTEGER    AS day_of_week,
    is_weekend::BOOLEAN     AS is_weekend

  FROM {{ ref('dates') }}  -- seeds

)

SELECT * FROM typed