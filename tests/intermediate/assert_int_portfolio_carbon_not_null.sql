select *
from {{ ref('int_portfolio_carbon') }}
where portfolio_id is null
   or security_id is null
   or portfolio_weight is null
   or carbon_intensity is null
   or weighted_carbon_intensity is null
