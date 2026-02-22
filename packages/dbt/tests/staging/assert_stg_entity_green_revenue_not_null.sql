SELECT *
FROM {{ ref('stg_entity_green_revenue') }}
WHERE entity_id IS NULL
   OR entity_name IS NULL
   OR green_revenue_usd IS NULL
   OR total_revenue_usd IS NULL
   OR green_revenue_share IS NULL
   OR as_of_date IS NULL
