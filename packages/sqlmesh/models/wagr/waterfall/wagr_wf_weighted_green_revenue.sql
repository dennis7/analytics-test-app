MODEL (
    name wagr.wf_weighted_green_revenue,
    kind FULL,
    audits (assert_wagr_wf_weighted_green_revenue_not_null),
    description 'Resolved green revenue shares weighted by portfolio position'
);

WITH resolved AS (
    SELECT * FROM wagr.wf_green_revenue
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
