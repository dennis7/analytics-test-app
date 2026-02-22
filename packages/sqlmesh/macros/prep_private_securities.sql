WITH positions AS (
    SELECT * FROM staging.stg_private_positions
),

security_master AS (
    SELECT * FROM staging.stg_security_master
)

SELECT
    p.portfolio_id,
    sm.internal_id,
    p.fund_id,
    sm.security_type,
    sm.sector,
    sm.entity_id,
    p.units_held,
    p.as_of_date
FROM positions p
INNER JOIN security_master sm
    ON p.fund_id = sm.fund_id
