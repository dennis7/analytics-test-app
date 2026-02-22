SELECT *
FROM {{ ref('wagr_met_portfolio_wagr') }}
WHERE portfolio_id IS NULL
   OR as_of_date IS NULL
   OR wagr IS NULL
   OR total_market_value IS NULL
   OR num_holdings IS NULL
   OR num_public IS NULL
   OR num_private IS NULL
