AUDIT (
    name assert_wagr_prep_public_securities_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR isin IS NULL
   OR security_type IS NULL
   OR sector IS NULL
   OR entity_id IS NULL
   OR quantity IS NULL
   OR as_of_date IS NULL
