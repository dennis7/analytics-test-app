select portfolio_id, count(*) as cnt
from {{ ref('fct_portfolio_waci') }}
group by portfolio_id
having count(*) > 1
