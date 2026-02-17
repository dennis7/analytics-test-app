select *
from {{ ref('stg_market_data') }}
where security_id is null
   or price is null
   or currency is null
   or as_of_date is null
