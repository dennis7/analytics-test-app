SELECT *
FROM {{ ref('wagr_prep_public_securities') }}
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR isin IS NULL
   OR security_type IS NULL
   OR sector IS NULL
   OR quantity IS NULL
   OR as_of_date IS NULL
