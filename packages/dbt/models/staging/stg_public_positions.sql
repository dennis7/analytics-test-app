SELECT
    portfolio_id::VARCHAR AS portfolio_id,
    isin::VARCHAR AS isin,
    quantity::DOUBLE AS quantity,
    as_of_date::DATE AS as_of_date
FROM {{ read_parquet('public_positions') }}
{% if var('reporting_date') %}
WHERE as_of_date = '{{ var("reporting_date") }}'::DATE
{% endif %}
