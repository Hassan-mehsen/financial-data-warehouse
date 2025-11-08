{{ 
   config(  
            on_schema_change='fail',
            on_configuration_change = 'fail',
            post_hook = create_index('date_key')
        )
}}

SELECT *
FROM {{ ref('stg_dim_date') }}