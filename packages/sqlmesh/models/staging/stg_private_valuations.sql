MODEL (
    name staging.stg_private_valuations,
    kind VIEW,
    audits (assert_stg_private_valuations_method_valid),
    description 'Staged private market NAV valuations'
);

SELECT
    fund_id::VARCHAR AS fund_id,
    nav_per_unit::DOUBLE AS nav_per_unit,
    valuation_method::VARCHAR AS valuation_method,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/private_valuations.parquet')
WHERE @REPORTING_DATE_FILTER()
