SELECT *
FROM {{ ref('stg_sector_carbon') }}
WHERE sector IS NULL
   OR avg_carbon_intensity IS NULL
   OR as_of_date IS NULL
