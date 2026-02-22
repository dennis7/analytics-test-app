SELECT
    internal_id::VARCHAR AS internal_id,
    isin::VARCHAR AS isin,
    fund_id::VARCHAR AS fund_id,
    security_type::VARCHAR AS security_type,
    sector::VARCHAR AS sector,
    entity_id::VARCHAR AS entity_id
FROM {{ read_parquet('security_master') }}
