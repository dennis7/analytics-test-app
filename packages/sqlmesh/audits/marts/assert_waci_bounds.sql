AUDIT (
    name assert_waci_bounds
);

SELECT
    portfolio_id,
    as_of_date,
    waci

FROM @this_model

WHERE waci < 0
