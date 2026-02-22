MODEL (
    name staging.stg_sector_green_revenue,
    kind VIEW,
    audits (assert_stg_sector_green_revenue_not_null),
    description 'Sector average green revenue shares'
);

SELECT
    sector::VARCHAR AS sector,
    avg_green_revenue_share::DOUBLE AS avg_green_revenue_share,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/sector_green_revenue.parquet')
WHERE @REPORTING_DATE_FILTER()
