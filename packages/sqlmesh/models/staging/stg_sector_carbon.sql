MODEL (
    name staging.stg_sector_carbon,
    kind VIEW,
    audits (assert_stg_sector_carbon_not_null),
    description 'Sector average carbon intensities'
);

SELECT
    sector::VARCHAR AS sector,
    avg_carbon_intensity::DOUBLE AS avg_carbon_intensity,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/sector_carbon.parquet')
WHERE @REPORTING_DATE_FILTER()
