MODEL (
    name staging.stg_entity_carbon,
    kind VIEW,
    audits (assert_stg_entity_carbon_not_null),
    description 'Company-level carbon emissions with derived intensity'
);

SELECT
    entity_id::VARCHAR AS entity_id,
    entity_name::VARCHAR AS entity_name,
    carbon_emissions_tonnes::DOUBLE AS carbon_emissions_tonnes,
    revenue_usd::DOUBLE AS revenue_usd,
    (carbon_emissions_tonnes / NULLIF(revenue_usd, 0))::DOUBLE AS carbon_intensity,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/entity_carbon.parquet')
WHERE @REPORTING_DATE_FILTER()
