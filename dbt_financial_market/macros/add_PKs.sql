
{% macro add_PKs(columns) %}
    
    {# 
      Add a PRIMARY KEY constraint to the current model (this).
      - columns : list of column names (['id', 'date_key'])
    #}
   
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 from pg_constraint 
            WHERE conname = 'pk_{{ this.identifier }}'
        ) 
        THEN
            EXECUTE format(
                'ALTER TABLE marts.{{ this.identifier }} ADD CONSTRAINT  pk_{{ this.identifier }} PRIMARY KEY ({{ columns | join(",") }})'
            );
        END IF;
    END$$;
    
{% endmacro %}

