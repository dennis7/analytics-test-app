AUDIT (
    name assert_fct_portfolio_waci_unique
);

SELECT portfolio_id, COUNT(*) AS cnt
FROM @this_model
GROUP BY portfolio_id
HAVING COUNT(*) > 1
