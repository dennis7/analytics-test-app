MODEL (
    name waci.met_portfolio_waci,
    kind FULL,
    audits (assert_waci_met_not_null, assert_waci_met_unique, assert_waci_met_bounds),
    description 'Weighted Average Carbon Intensity per portfolio'
);

WITH weighted AS (
    SELECT * FROM waci.wf_weighted_carbon
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
