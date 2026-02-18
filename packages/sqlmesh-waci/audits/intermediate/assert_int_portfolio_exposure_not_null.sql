AUDIT (
    name assert_int_portfolio_exposure_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR security_id IS NULL
   OR market_value IS NULL
   OR portfolio_weight IS NULL
   OR as_of_date IS NULL
