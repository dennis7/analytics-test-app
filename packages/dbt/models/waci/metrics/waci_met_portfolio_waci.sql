{{ config(alias='met_portfolio_waci') }}

WITH weighted AS (
    SELECT * FROM {{ ref('waci_wf_weighted_carbon') }}
)

SELECT
    portfolio_id,
    as_of_date,
    SUM(weighted_carbon_intensity)::DOUBLE AS waci,
    SUM(market_value)::DOUBLE AS total_market_value,
    COUNT(*)::BIGINT AS num_holdings,
    COUNT(*) FILTER (WHERE security_type != 'private_equity')::BIGINT AS num_public,
    COUNT(*) FILTER (WHERE security_type = 'private_equity')::BIGINT AS num_private
FROM weighted
GROUP BY portfolio_id, as_of_date
