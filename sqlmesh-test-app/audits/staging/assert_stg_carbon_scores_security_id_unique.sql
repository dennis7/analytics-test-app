AUDIT (
    name assert_stg_carbon_scores_security_id_unique
);

SELECT security_id, COUNT(*) AS cnt
FROM @this_model
GROUP BY security_id
HAVING COUNT(*) > 1
