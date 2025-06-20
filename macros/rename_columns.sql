{% macro rename_columns(model_name) %}
    {# Load rename entries #}
    {% set rename_entries = run_query(
        "SELECT existing_column_name, new_column_name 
         FROM raw.schema_change_metadata 
         WHERE update_flag = 'ACTIVE' 
         AND existing_model_name = '" ~ model_name ~ "'"
    ) %}
    
    {% if rename_entries['rows'] | length == 0 %}
        {{ log("No active rename entries for model " ~ model_name, info=True) }}
        {% do return("No changes") %}
    {% endif %}

    {# Describe the original table to get columns #}
    {% set columns_info = run_query("DESCRIBE VIEW " ~ model_name) %}
    {% set original_columns = columns_info.columns[0].values() %}

    {% set select_columns = [] %}
    {% for col in original_columns %}
        {% set rename_index = rename_entries.columns[0].values().index(col) if col in rename_entries.columns[0].values() else -1 %}
        {% if rename_index >= 0 %}
            {% set new_col_name = rename_entries.columns[1].values()[rename_index] %}
            {% do select_columns.append(col ~ " AS " ~ new_col_name) %}
        {% else %}
            {% do select_columns.append(col) %}
        {% endif %}
    {% endfor %}

    {# Create new view definition #}
    {% set view_sql %}
        CREATE OR REPLACE VIEW {{ model_name }} AS
        SELECT {{ select_columns | join(', ') }} FROM {{ model_name }};
    {% endset %}

    {{ log("Recreating view with column renames: " ~ view_sql, info=True) }}
    {% do run_query(view_sql) %}

    {# Mark metadata entries as handled #}
    {% do run_query(
        "UPDATE raw.schema_change_metadata 
         SET update_flag = 'INACTIVE' 
         WHERE update_flag = 'ACTIVE' 
         AND existing_model_name = '" ~ model_name ~ "'"
    ) %}

    {{ log("Schema changes applied and metadata updated.", info=True) }}
{% endmacro %}
