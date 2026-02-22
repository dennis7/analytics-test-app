AUDIT (
    name assert_stg_entity_carbon_not_null
);

SELECT *
FROM @this_model
WHERE entity_id IS NULL
   OR entity_name IS NULL
   OR carbon_emissions_tonnes IS NULL
   OR revenue_usd IS NULL
   OR carbon_intensity IS NULL
   OR as_of_date IS NULL
