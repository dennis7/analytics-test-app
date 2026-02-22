MODEL (
    name staging.stg_security_master,
    kind VIEW,
    audits (
        assert_stg_security_master_id_unique,
        assert_stg_security_master_type_valid
    ),
    description 'Security identifier mapping and classification'
);

SELECT
    internal_id::VARCHAR AS internal_id,
    isin::VARCHAR AS isin,
    fund_id::VARCHAR AS fund_id,
    security_type::VARCHAR AS security_type,
    sector::VARCHAR AS sector,
    entity_id::VARCHAR AS entity_id
FROM read_parquet(@data_path || '/security_master.parquet')
