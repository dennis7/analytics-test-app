select *
from {{ ref('stg_market_data') }}
where currency not in ('USD', 'EUR', 'GBP', 'JPY')
