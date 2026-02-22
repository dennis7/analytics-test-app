{% macro shared_prep_public_securities() %}

WITH positions AS (
    SELECT * FROM {{ ref('stg_public_positions') }}
),

security_master AS (
    SELECT * FROM {{ ref('stg_security_master') }}
)

SELECT
    p.portfolio_id,
    sm.internal_id,
    p.isin,
    sm.security_type,
    sm.sector,
    sm.entity_id,
    p.quantity,
    p.as_of_date
FROM positions p
INNER JOIN security_master sm
    ON p.isin = sm.isin

{% endmacro %}
