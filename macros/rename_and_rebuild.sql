{% macro rename_and_rebuild(model_name, source_table) %}

  {# Get the rename entries for the given model from metadata table #}
  {% set rename_entries_query = "SELECT existing_column_name, new_column_name FROM raw.schema_change_metadata WHERE existing_model_name = '" ~ model_name ~ "' AND update_flag = 'ACTIVE'" %}
  {% set rename_entries = run_query(rename_entries_query) %}

  {% if rename_entries is none or rename_entries.columns|length == 0 %}
    {{ log("No active rename entries for model " ~ model_name, info=True) }}
    {% do exceptions.raise_compiler_error("No active rename entries for model " ~ model_name) %}
  {% endif %}

  {# Get original columns for the source table #}
  {% set describe_query = "DESCRIBE TABLE " ~ source_table %}
  {% set describe_result = run_query(describe_query) %}
  {% set original_columns = [] %}
  {% for row in describe_result.rows %}
    {% do original_columns.append(row['name']) %}
  {% endfor %}

  {# Build select clause with renamed columns #}
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

  {# Construct final SQL to recreate view #}
  {% set select_sql = select_columns | join(", ") %}
  {% set view_sql = "CREATE OR REPLACE VIEW " ~ model_name ~ " AS SELECT " ~ select_sql ~ " FROM " ~ source_table %}

  {{ log("Recreating view with renamed columns:\n" ~ view_sql, info=True) }}

  {# Run the query to recreate view #}
  {% do run_query(view_sql) %}

  {# After successful view creation, mark entries as INACTIVE #}
  {% for i in range(rename_entries.columns[0].values() | length) %}
    {% set existing_col = rename_entries.columns[0].values()[i] %}
    {% set new_col = rename_entries.columns[1].values()[i] %}
    {% set update_sql %}
      UPDATE raw.schema_change_metadata
      SET update_flag = 'INACTIVE'
      WHERE 
        existing_model_name = '{{ model_name }}'
        AND existing_column_name = '{{ existing_col }}'
        AND new_column_name = '{{ new_col }}'
        AND update_flag = 'ACTIVE';
    {% endset %}
    {% do run_query(update_sql) %}
  {% endfor %}

{% endmacro %}
