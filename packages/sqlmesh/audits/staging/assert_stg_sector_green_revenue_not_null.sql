AUDIT (
    name assert_stg_sector_green_revenue_not_null
);

SELECT *
FROM @this_model
WHERE sector IS NULL
   OR avg_green_revenue_share IS NULL
   OR as_of_date IS NULL
