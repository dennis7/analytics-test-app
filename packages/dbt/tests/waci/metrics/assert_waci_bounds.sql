SELECT *
FROM {{ ref('waci_met_portfolio_waci') }}
WHERE waci < 0
