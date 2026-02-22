SELECT *
FROM {{ ref('stg_entity_carbon') }}
WHERE entity_id IS NULL
   OR entity_name IS NULL
   OR carbon_emissions_tonnes IS NULL
   OR revenue_usd IS NULL
   OR carbon_intensity IS NULL
   OR as_of_date IS NULL
