select security_id, count(*) as cnt
from {{ ref('stg_carbon_scores') }}
group by security_id
having count(*) > 1
