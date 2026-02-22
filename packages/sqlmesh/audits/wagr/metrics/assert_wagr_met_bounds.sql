AUDIT (
    name assert_wagr_met_bounds
);

SELECT *
FROM @this_model
WHERE wagr < 0 OR wagr > 1
