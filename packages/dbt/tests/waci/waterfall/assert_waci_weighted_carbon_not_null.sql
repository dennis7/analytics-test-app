SELECT *
FROM {{ ref('waci_wf_weighted_carbon') }}
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR resolved_carbon_intensity IS NULL
   OR weighted_carbon_intensity IS NULL
   OR resolution_method IS NULL
   OR as_of_date IS NULL
