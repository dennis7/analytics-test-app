with exposure as (

    select * from {{ ref('int_portfolio_exposure') }}

),

carbon as (

    select * from {{ ref('stg_carbon_scores') }}

)

select
    e.portfolio_id,
    e.security_id,
    e.quantity,
    e.price,
    e.market_value,
    e.portfolio_weight,
    c.company_name,
    c.carbon_emissions_tonnes,
    c.revenue_usd,
    c.carbon_intensity,
    (e.portfolio_weight * c.carbon_intensity)::double as weighted_carbon_intensity,
    e.as_of_date

from exposure e
inner join carbon c
    on e.security_id = c.security_id
    and e.as_of_date = c.as_of_date
