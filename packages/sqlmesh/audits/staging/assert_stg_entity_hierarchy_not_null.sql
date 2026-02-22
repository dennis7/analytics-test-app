AUDIT (
    name assert_stg_entity_hierarchy_not_null
);

SELECT *
FROM @this_model
WHERE child_entity_id IS NULL
   OR parent_entity_id IS NULL
