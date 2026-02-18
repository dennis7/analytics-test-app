AUDIT (
    name assert_int_portfolio_carbon_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR security_id IS NULL
   OR portfolio_weight IS NULL
   OR carbon_intensity IS NULL
   OR weighted_carbon_intensity IS NULL
