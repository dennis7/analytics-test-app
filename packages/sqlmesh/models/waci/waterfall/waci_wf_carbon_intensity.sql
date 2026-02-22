MODEL (
    name waci.wf_carbon_intensity,
    kind FULL,
    audits (assert_waci_wf_intensity_resolved, assert_waci_wf_resolution_method_valid),
    description 'Carbon intensity resolved via waterfall: direct, parent, sector'
);

WITH weights AS (
    SELECT * FROM waci.harm_portfolio_weights
),

entity_carbon AS (
    SELECT * FROM staging.stg_entity_carbon
),

hierarchy AS (
    SELECT * FROM staging.stg_entity_hierarchy
),

sector_carbon AS (
    SELECT * FROM staging.stg_sector_carbon
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
        direct.carbon_intensity,
        parent_carbon.carbon_intensity,
        sc.avg_carbon_intensity
    )::DOUBLE AS resolved_carbon_intensity,
    CASE
        WHEN direct.carbon_intensity IS NOT NULL THEN 'direct'
        WHEN parent_carbon.carbon_intensity IS NOT NULL THEN 'parent'
        ELSE 'sector'
    END::VARCHAR AS resolution_method,
    COALESCE(
        direct.entity_id,
        parent_carbon.entity_id,
        NULL
    )::VARCHAR AS resolution_entity_id,
    w.as_of_date
FROM weights w
LEFT JOIN entity_carbon direct
    ON w.entity_id = direct.entity_id
    AND w.as_of_date = direct.as_of_date
LEFT JOIN hierarchy h
    ON w.entity_id = h.child_entity_id
LEFT JOIN entity_carbon parent_carbon
    ON h.parent_entity_id = parent_carbon.entity_id
    AND w.as_of_date = parent_carbon.as_of_date
LEFT JOIN sector_carbon sc
    ON w.sector = sc.sector
    AND w.as_of_date = sc.as_of_date
