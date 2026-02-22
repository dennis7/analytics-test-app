{{ config(alias='met_portfolio_wagr') }}

WITH weighted AS (
    SELECT * FROM {{ ref('wagr_wf_weighted_green_revenue') }}
)

SELECT
    portfolio_id,
    as_of_date,
    SUM(weighted_green_revenue_share)::DOUBLE AS wagr,
    SUM(market_value)::DOUBLE AS total_market_value,
    COUNT(*)::BIGINT AS num_holdings,
    COUNT(*) FILTER (WHERE security_type != 'private_equity')::BIGINT AS num_public,
    COUNT(*) FILTER (WHERE security_type = 'private_equity')::BIGINT AS num_private
FROM weighted
GROUP BY portfolio_id, as_of_date
