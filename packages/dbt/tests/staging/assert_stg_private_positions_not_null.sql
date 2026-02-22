SELECT *
FROM {{ ref('stg_private_positions') }}
WHERE portfolio_id IS NULL
   OR fund_id IS NULL
   OR units_held IS NULL
   OR as_of_date IS NULL
