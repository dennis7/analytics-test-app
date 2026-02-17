with positions as (

    select * from {{ ref('stg_portfolio_positions') }}

),

market_data as (

    select * from {{ ref('stg_market_data') }}

),

valued_positions as (

    select
        p.portfolio_id,
        p.security_id,
        p.quantity,
        m.price,
        m.currency,
        (p.quantity * m.price)::double as market_value,
        p.as_of_date

    from positions p
    inner join market_data m
        on p.security_id = m.security_id
        and p.as_of_date = m.as_of_date

)

select
    portfolio_id,
    security_id,
    quantity,
    price,
    currency,
    market_value,
    (market_value / sum(market_value) over (
        partition by portfolio_id, as_of_date
    ))::double as portfolio_weight,
    as_of_date

from valued_positions
