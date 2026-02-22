SELECT *
FROM {{ ref('stg_public_positions') }}
WHERE portfolio_id IS NULL
   OR isin IS NULL
   OR quantity IS NULL
   OR as_of_date IS NULL
