{{ config(alias='wf_weighted_carbon') }}

WITH resolved AS (
    SELECT * FROM {{ ref('waci_wf_carbon_intensity') }}
)

SELECT
    portfolio_id,
    internal_id,
    security_type,
    sector,
    entity_id,
    market_value,
    portfolio_weight,
    resolved_carbon_intensity,
    resolution_method,
    resolution_entity_id,
    (portfolio_weight * resolved_carbon_intensity)::DOUBLE AS weighted_carbon_intensity,
    as_of_date
FROM resolved
