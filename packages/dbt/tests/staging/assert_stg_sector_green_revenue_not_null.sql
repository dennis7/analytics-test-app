SELECT *
FROM {{ ref('stg_sector_green_revenue') }}
WHERE sector IS NULL
   OR avg_green_revenue_share IS NULL
   OR as_of_date IS NULL
