{% macro read_parquet(file_name) %}
    read_parquet('{{ var("data_path") }}/{{ file_name }}.parquet')
{% endmacro %}
