SELECT
    entity_id::VARCHAR AS entity_id,
    entity_name::VARCHAR AS entity_name,
    green_revenue_usd::DOUBLE AS green_revenue_usd,
    total_revenue_usd::DOUBLE AS total_revenue_usd,
    (green_revenue_usd / NULLIF(total_revenue_usd, 0))::DOUBLE AS green_revenue_share,
    as_of_date::DATE AS as_of_date
FROM {{ read_parquet('entity_green_revenue') }}
{% if var('reporting_date') %}
WHERE as_of_date = '{{ var("reporting_date") }}'::DATE
{% endif %}
