MODEL (
    name wagr.wf_green_revenue,
    kind FULL,
    audits (assert_wagr_wf_green_revenue_resolved, assert_wagr_wf_resolution_method_valid),
    description 'Green revenue share resolved via waterfall: direct, parent, sector'
);

WITH weights AS (
    SELECT * FROM wagr.harm_portfolio_weights
),

entity_green_revenue AS (
    SELECT * FROM staging.stg_entity_green_revenue
),

hierarchy AS (
    SELECT * FROM staging.stg_entity_hierarchy
),

sector_green_revenue AS (
    SELECT * FROM staging.stg_sector_green_revenue
)

SELECT
    w.portfolio_id,
    w.internal_id,
    w.security_type,
    w.sector,
    w.entity_id,
    w.market_value,
    w.portfolio_weight,
    COALESCE(
        direct.green_revenue_share,
        parent_green.green_revenue_share,
        sg.avg_green_revenue_share
    )::DOUBLE AS resolved_green_revenue_share,
    CASE
        WHEN direct.green_revenue_share IS NOT NULL THEN 'direct'
        WHEN parent_green.green_revenue_share IS NOT NULL THEN 'parent'
        ELSE 'sector'
    END::VARCHAR AS resolution_method,
    COALESCE(
        direct.entity_id,
        parent_green.entity_id,
        NULL
    )::VARCHAR AS resolution_entity_id,
    w.as_of_date
FROM weights w
LEFT JOIN entity_green_revenue direct
    ON w.entity_id = direct.entity_id
    AND w.as_of_date = direct.as_of_date
LEFT JOIN hierarchy h
    ON w.entity_id = h.child_entity_id
LEFT JOIN entity_green_revenue parent_green
    ON h.parent_entity_id = parent_green.entity_id
    AND w.as_of_date = parent_green.as_of_date
LEFT JOIN sector_green_revenue sg
    ON w.sector = sg.sector
    AND w.as_of_date = sg.as_of_date
