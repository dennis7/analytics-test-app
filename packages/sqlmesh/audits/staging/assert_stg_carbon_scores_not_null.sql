AUDIT (
    name assert_stg_carbon_scores_not_null
);

SELECT *
FROM @this_model
WHERE security_id IS NULL
   OR company_name IS NULL
   OR carbon_emissions_tonnes IS NULL
   OR revenue_usd IS NULL
   OR carbon_intensity IS NULL
   OR as_of_date IS NULL
