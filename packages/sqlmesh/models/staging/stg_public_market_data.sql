MODEL (
    name staging.stg_public_market_data,
    kind VIEW,
    audits (
        assert_stg_public_market_data_not_null,
        assert_stg_public_market_data_isin_unique,
        assert_stg_public_market_data_currency_valid
    ),
    description 'Staged public market prices'
);

SELECT
    isin::VARCHAR AS isin,
    price::DOUBLE AS price,
    currency::VARCHAR AS currency,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/public_market_data.parquet')
WHERE @REPORTING_DATE_FILTER()
