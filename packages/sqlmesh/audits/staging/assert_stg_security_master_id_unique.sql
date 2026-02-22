AUDIT (
    name assert_stg_security_master_id_unique
);

SELECT internal_id, COUNT(*) AS cnt
FROM @this_model
GROUP BY internal_id
HAVING COUNT(*) > 1
