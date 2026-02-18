with source as (

    select * from {{ read_parquet('carbon_scores') }}

)

select
    security_id::varchar as security_id,
    company_name::varchar as company_name,
    carbon_emissions_tonnes::double as carbon_emissions_tonnes,
    revenue_usd::double as revenue_usd,
    (carbon_emissions_tonnes / nullif(revenue_usd, 0))::double as carbon_intensity,
    as_of_date::date as as_of_date

from source
