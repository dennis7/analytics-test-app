MODEL (
    name staging.stg_carbon_scores,
    kind VIEW,
    audits (
        assert_stg_carbon_scores_not_null,
        assert_stg_carbon_scores_security_id_unique
    ),
    description 'Staged carbon scores with derived carbon intensity'
);

@DEF(data_path, 'data/dev');

SELECT
    security_id::VARCHAR AS security_id,
    company_name::VARCHAR AS company_name,
    carbon_emissions_tonnes::DOUBLE AS carbon_emissions_tonnes,
    revenue_usd::DOUBLE AS revenue_usd,
    (carbon_emissions_tonnes / NULLIF(revenue_usd, 0))::DOUBLE AS carbon_intensity,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/carbon_scores.parquet')
