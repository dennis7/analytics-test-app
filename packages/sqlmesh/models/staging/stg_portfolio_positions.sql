MODEL (
    name staging.stg_portfolio_positions,
    kind VIEW,
    audits (assert_stg_portfolio_positions_not_null),
    description 'Staged portfolio positions with typed columns'
);

SELECT
    portfolio_id::VARCHAR AS portfolio_id,
    security_id::VARCHAR AS security_id,
    quantity::DOUBLE AS quantity,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/portfolio_positions.parquet')
