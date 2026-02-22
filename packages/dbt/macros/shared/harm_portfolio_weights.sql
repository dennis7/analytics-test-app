{% macro shared_harm_portfolio_weights(metric) %}

WITH valued AS (
    SELECT * FROM {{ ref(metric ~ '_harm_valued_positions') }}
)

SELECT
    portfolio_id,
    internal_id,
    security_type,
    sector,
    entity_id,
    market_value,
    (market_value / SUM(market_value) OVER (
        PARTITION BY portfolio_id, as_of_date
    ))::DOUBLE AS portfolio_weight,
    as_of_date
FROM valued

{% endmacro %}
