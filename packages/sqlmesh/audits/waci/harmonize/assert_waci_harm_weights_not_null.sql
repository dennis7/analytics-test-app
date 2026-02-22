AUDIT (
    name assert_waci_harm_weights_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR security_type IS NULL
   OR sector IS NULL
   OR entity_id IS NULL
   OR market_value IS NULL
   OR portfolio_weight IS NULL
   OR as_of_date IS NULL
