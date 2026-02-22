MODEL (
    name staging.stg_private_positions,
    kind VIEW,
    audits (assert_stg_private_positions_not_null),
    description 'Staged private market portfolio positions'
);

SELECT
    portfolio_id::VARCHAR AS portfolio_id,
    fund_id::VARCHAR AS fund_id,
    units_held::DOUBLE AS units_held,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/private_positions.parquet')
WHERE @REPORTING_DATE_FILTER()
