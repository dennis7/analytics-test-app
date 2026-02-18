-- WACI should always be non-negative (emissions and revenue are positive)
select
    portfolio_id,
    as_of_date,
    waci

from {{ ref('fct_portfolio_waci') }}

where waci < 0
