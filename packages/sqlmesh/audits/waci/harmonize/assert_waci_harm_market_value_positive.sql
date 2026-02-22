AUDIT (
    name assert_waci_harm_market_value_positive
);

SELECT *
FROM @this_model
WHERE market_value IS NULL
   OR market_value <= 0
