AUDIT (
    name assert_stg_sector_carbon_not_null
);

SELECT *
FROM @this_model
WHERE sector IS NULL
   OR avg_carbon_intensity IS NULL
   OR as_of_date IS NULL
