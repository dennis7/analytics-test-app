MODEL (
    name staging.stg_entity_hierarchy,
    kind VIEW,
    audits (assert_stg_entity_hierarchy_not_null),
    description 'Subsidiary to parent entity mapping'
);

SELECT
    child_entity_id::VARCHAR AS child_entity_id,
    parent_entity_id::VARCHAR AS parent_entity_id
FROM read_parquet(@data_path || '/entity_hierarchy.parquet')
