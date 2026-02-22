SELECT
    entity_id::VARCHAR AS entity_id,
    entity_name::VARCHAR AS entity_name,
    carbon_emissions_tonnes::DOUBLE AS carbon_emissions_tonnes,
    revenue_usd::DOUBLE AS revenue_usd,
    (carbon_emissions_tonnes / NULLIF(revenue_usd, 0))::DOUBLE AS carbon_intensity,
    as_of_date::DATE AS as_of_date
FROM {{ read_parquet('entity_carbon') }}
{% if var('reporting_date') %}
WHERE as_of_date = '{{ var("reporting_date") }}'::DATE
{% endif %}
