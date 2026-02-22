AUDIT (
    name assert_stg_public_market_data_not_null
);

SELECT *
FROM @this_model
WHERE isin IS NULL
   OR price IS NULL
   OR currency IS NULL
   OR as_of_date IS NULL
