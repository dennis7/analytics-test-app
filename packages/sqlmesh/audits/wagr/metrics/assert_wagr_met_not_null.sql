AUDIT (
    name assert_wagr_met_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR as_of_date IS NULL
   OR wagr IS NULL
   OR total_market_value IS NULL
   OR num_holdings IS NULL
   OR num_public IS NULL
   OR num_private IS NULL
