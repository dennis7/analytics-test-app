select *
from {{ ref('fct_portfolio_waci') }}
where portfolio_id is null
   or as_of_date is null
   or waci is null
   or total_market_value is null
   or num_holdings is null
