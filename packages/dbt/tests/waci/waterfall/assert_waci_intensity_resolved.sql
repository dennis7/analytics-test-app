SELECT *
FROM {{ ref('waci_wf_carbon_intensity') }}
WHERE resolved_carbon_intensity IS NULL
