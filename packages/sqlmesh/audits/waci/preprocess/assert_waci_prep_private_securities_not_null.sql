AUDIT (
    name assert_waci_prep_private_securities_not_null
);

SELECT *
FROM @this_model
WHERE portfolio_id IS NULL
   OR internal_id IS NULL
   OR fund_id IS NULL
   OR security_type IS NULL
   OR sector IS NULL
   OR entity_id IS NULL
   OR units_held IS NULL
   OR as_of_date IS NULL
