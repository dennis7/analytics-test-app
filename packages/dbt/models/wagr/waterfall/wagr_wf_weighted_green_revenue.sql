{{ config(alias='wf_weighted_green_revenue') }}

WITH resolved AS (
    SELECT * FROM {{ ref('wagr_wf_green_revenue') }}
)

SELECT
    portfolio_id,
    internal_id,
    security_type,
    sector,
    entity_id,
    market_value,
    portfolio_weight,
    resolved_green_revenue_share,
    resolution_method,
    resolution_entity_id,
    (portfolio_weight * resolved_green_revenue_share)::DOUBLE AS weighted_green_revenue_share,
    as_of_date
FROM resolved
