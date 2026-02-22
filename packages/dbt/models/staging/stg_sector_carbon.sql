SELECT
    sector::VARCHAR AS sector,
    avg_carbon_intensity::DOUBLE AS avg_carbon_intensity,
    as_of_date::DATE AS as_of_date
FROM {{ read_parquet('sector_carbon') }}
{% if var('reporting_date') %}
WHERE as_of_date = '{{ var("reporting_date") }}'::DATE
{% endif %}
