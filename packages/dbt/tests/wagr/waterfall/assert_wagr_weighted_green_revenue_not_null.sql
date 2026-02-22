SELECT *
FROM {{ ref('wagr_wf_weighted_green_revenue') }}
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR resolved_green_revenue_share IS NULL
   OR weighted_green_revenue_share IS NULL
   OR resolution_method IS NULL
   OR as_of_date IS NULL
