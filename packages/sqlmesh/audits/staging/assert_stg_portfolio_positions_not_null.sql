AUDIT (
    name assert_stg_portfolio_positions_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR security_id IS NULL
   OR quantity IS NULL
   OR as_of_date IS NULL
