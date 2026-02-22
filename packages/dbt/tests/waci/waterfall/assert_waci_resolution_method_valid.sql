SELECT *
FROM {{ ref('waci_wf_carbon_intensity') }}
WHERE resolution_method NOT IN ('direct', 'parent', 'sector')
