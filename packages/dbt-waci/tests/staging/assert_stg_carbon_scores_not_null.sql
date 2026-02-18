select *
from {{ ref('stg_carbon_scores') }}
where security_id is null
   or company_name is null
   or carbon_emissions_tonnes is null
   or revenue_usd is null
   or carbon_intensity is null
   or as_of_date is null
