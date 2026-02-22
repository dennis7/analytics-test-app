AUDIT (
    name assert_stg_private_positions_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR fund_id IS NULL
   OR units_held IS NULL
   OR as_of_date IS NULL
