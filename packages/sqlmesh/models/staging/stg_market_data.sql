MODEL (
    name staging.stg_market_data,
    kind VIEW,
    audits (
        assert_stg_market_data_not_null,
        assert_stg_market_data_security_id_unique,
        assert_stg_market_data_currency_valid
    ),
    description 'Staged market prices with typed columns'
);

SELECT
    security_id::VARCHAR AS security_id,
    price::DOUBLE AS price,
    currency::VARCHAR AS currency,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/market_data.parquet')
