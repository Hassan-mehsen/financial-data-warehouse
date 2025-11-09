
{% macro create_index(column_name) %}
    
    {#
      Create an index on a given column.
      - column_name: single column name (string)
    #}

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 from pg_indexes 
            WHERE indexname = 'idx_{{ this.identifier }}_{{ column_name }}'
        ) 
        THEN
            EXECUTE format(
                'CREATE INDEX idx_{{ this.identifier }}_{{ column_name }} ON marts.{{ this.identifier }} ({{column_name}})'
            );
        END IF;
    END$$;

{% endmacro %}

