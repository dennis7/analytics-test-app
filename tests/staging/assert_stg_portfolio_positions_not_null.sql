select *
from {{ ref('stg_portfolio_positions') }}
where portfolio_id is null
   or security_id is null
   or quantity is null
   or as_of_date is null
