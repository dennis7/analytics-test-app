SELECT portfolio_id, as_of_date, COUNT(*) AS cnt
FROM {{ ref('wagr_met_portfolio_wagr') }}
GROUP BY portfolio_id, as_of_date
HAVING COUNT(*) > 1
