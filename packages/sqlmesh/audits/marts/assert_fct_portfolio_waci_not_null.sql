AUDIT (
    name assert_fct_portfolio_waci_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR as_of_date IS NULL
   OR waci IS NULL
   OR total_market_value IS NULL
   OR num_holdings IS NULL
