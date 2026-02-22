SELECT
    sector::VARCHAR AS sector,
    avg_green_revenue_share::DOUBLE AS avg_green_revenue_share,
    as_of_date::DATE AS as_of_date
FROM {{ read_parquet('sector_green_revenue') }}
{% if var('reporting_date') %}
WHERE as_of_date = '{{ var("reporting_date") }}'::DATE
{% endif %}
