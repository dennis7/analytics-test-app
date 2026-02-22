MODEL (
    name staging.stg_entity_green_revenue,
    kind VIEW,
    audits (assert_stg_entity_green_revenue_not_null),
    description 'Company-level green revenue with derived share'
);

SELECT
    entity_id::VARCHAR AS entity_id,
    entity_name::VARCHAR AS entity_name,
    green_revenue_usd::DOUBLE AS green_revenue_usd,
    total_revenue_usd::DOUBLE AS total_revenue_usd,
    (green_revenue_usd / NULLIF(total_revenue_usd, 0))::DOUBLE AS green_revenue_share,
    as_of_date::DATE AS as_of_date
FROM read_parquet(@data_path || '/entity_green_revenue.parquet')
WHERE @REPORTING_DATE_FILTER()
