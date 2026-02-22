SELECT portfolio_id, as_of_date, COUNT(*) AS cnt
FROM {{ ref('waci_met_portfolio_waci') }}
GROUP BY portfolio_id, as_of_date
HAVING COUNT(*) > 1
