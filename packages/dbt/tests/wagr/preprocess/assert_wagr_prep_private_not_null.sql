SELECT *
FROM {{ ref('wagr_prep_private_securities') }}
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR fund_id IS NULL
   OR security_type IS NULL
   OR sector IS NULL
   OR units_held IS NULL
   OR as_of_date IS NULL
