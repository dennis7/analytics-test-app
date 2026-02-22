SELECT
    isin::VARCHAR AS isin,
    price::DOUBLE AS price,
    currency::VARCHAR AS currency,
    as_of_date::DATE AS as_of_date
FROM {{ read_parquet('public_market_data') }}
{% if var('reporting_date') %}
WHERE as_of_date = '{{ var("reporting_date") }}'::DATE
{% endif %}
