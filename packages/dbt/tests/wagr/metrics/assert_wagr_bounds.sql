SELECT *
FROM {{ ref('wagr_met_portfolio_wagr') }}
WHERE wagr < 0 OR wagr > 1
