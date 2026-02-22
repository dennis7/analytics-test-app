AUDIT (
    name assert_waci_met_bounds
);

SELECT *
FROM @this_model
WHERE waci < 0
