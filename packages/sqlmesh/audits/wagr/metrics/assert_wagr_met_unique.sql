AUDIT (
    name assert_wagr_met_unique
);

SELECT portfolio_id, as_of_date, COUNT(*) AS cnt
FROM @this_model
GROUP BY portfolio_id, as_of_date
HAVING COUNT(*) > 1
