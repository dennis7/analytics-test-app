AUDIT (
    name assert_stg_security_master_type_valid
);

SELECT *
FROM @this_model
WHERE security_type NOT IN ('equity', 'bond', 'private_equity')
