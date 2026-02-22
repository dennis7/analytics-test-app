MODEL (
    name staging.stg_public_positions,
    kind VIEW,
    audits (assert_stg_public_positions_not_null),
    description 'Staged public market portfolio positions'
);

SELECT
    portfolio_id::VARCHAR AS portfolio_id,
    isin::VARCHAR AS isin,
    quantity::DOUBLE AS quantity,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/public_positions.parquet')
WHERE @REPORTING_DATE_FILTER()
