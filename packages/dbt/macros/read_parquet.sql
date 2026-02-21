{% macro read_parquet(file_name) %}
    read_parquet('{{ env_var("DATA_DIR") }}/{{ target.name }}/input/{{ file_name }}.parquet')
{% endmacro %}
