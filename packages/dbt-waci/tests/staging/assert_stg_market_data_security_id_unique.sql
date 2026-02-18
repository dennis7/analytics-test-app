select security_id, count(*) as cnt
from {{ ref('stg_market_data') }}
group by security_id
having count(*) > 1
