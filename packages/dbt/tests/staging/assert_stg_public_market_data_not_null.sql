SELECT *
FROM {{ ref('stg_public_market_data') }}
WHERE isin IS NULL
   OR price IS NULL
   OR currency IS NULL
   OR as_of_date IS NULL
