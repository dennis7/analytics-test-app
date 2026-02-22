SELECT *
FROM {{ ref('stg_entity_hierarchy') }}
WHERE child_entity_id IS NULL
   OR parent_entity_id IS NULL
