AUDIT (
    name assert_waci_wf_weighted_carbon_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR market_value IS NULL
   OR portfolio_weight IS NULL
   OR resolved_carbon_intensity IS NULL
   OR weighted_carbon_intensity IS NULL
   OR as_of_date IS NULL
