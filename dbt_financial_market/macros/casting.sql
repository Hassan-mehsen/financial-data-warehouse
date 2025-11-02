
{% macro bigint_safe_cast_dict(columns_map) %}
    
    {#
    Apply bigint_safe_cast to multiple JSON fields at once.

    Args:
        columns_map (dict): Mapping between JSON extraction expressions and aliases.

    - Converts empty string to NULL
    - Rounds numeric values before  casting
    
    Example:
        {{ bigint_safe_cast_dict({
            "elem->>'totalAssets'": "total_assets",
            "elem->>'netIncome'": "net_income"
        }) }}
    #}

    {%- for col, alias in columns_map.items() %}

        ROUND(NULLIF( ({{ col }}), '' )::numeric, 0)::bigint AS {{ alias }}
        
        {%- if not loop.last %}
        ,
        {% endif -%}
    
    {%- endfor %}
{% endmacro %}
