SELECT
    fund_id::VARCHAR AS fund_id,
    nav_per_unit::DOUBLE AS nav_per_unit,
    valuation_method::VARCHAR AS valuation_method,
    as_of_date::DATE AS as_of_date
FROM {{ read_parquet('private_valuations') }}
{% if var('reporting_date') %}
WHERE as_of_date = '{{ var("reporting_date") }}'::DATE
{% endif %}
