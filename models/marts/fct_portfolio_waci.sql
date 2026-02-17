with portfolio_carbon as (

    select * from {{ ref('int_portfolio_carbon') }}

)

select
    portfolio_id,
    as_of_date,
    sum(weighted_carbon_intensity)::double as waci,
    sum(market_value)::double as total_market_value,
    count(*)::bigint as num_holdings

from portfolio_carbon
group by portfolio_id, as_of_date
