AUDIT (
    name assert_wagr_wf_weighted_green_revenue_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR market_value IS NULL
   OR portfolio_weight IS NULL
   OR resolved_green_revenue_share IS NULL
   OR weighted_green_revenue_share IS NULL
   OR as_of_date IS NULL
