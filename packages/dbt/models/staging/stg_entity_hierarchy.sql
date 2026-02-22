SELECT
    child_entity_id::VARCHAR AS child_entity_id,
    parent_entity_id::VARCHAR AS parent_entity_id
FROM {{ read_parquet('entity_hierarchy') }}
