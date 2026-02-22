SELECT *
FROM {{ ref('waci_harm_portfolio_weights') }}
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR market_value IS NULL
   OR portfolio_weight IS NULL
   OR as_of_date IS NULL
