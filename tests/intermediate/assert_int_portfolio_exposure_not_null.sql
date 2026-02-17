select *
from {{ ref('int_portfolio_exposure') }}
where portfolio_id is null
   or security_id is null
   or market_value is null
   or portfolio_weight is null
   or as_of_date is null
