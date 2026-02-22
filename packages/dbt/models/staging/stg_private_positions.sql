SELECT
    portfolio_id::VARCHAR AS portfolio_id,
    fund_id::VARCHAR AS fund_id,
    units_held::DOUBLE AS units_held,
    as_of_date::DATE AS as_of_date
FROM {{ read_parquet('private_positions') }}
{% if var('reporting_date') %}
WHERE as_of_date = '{{ var("reporting_date") }}'::DATE
{% endif %}
