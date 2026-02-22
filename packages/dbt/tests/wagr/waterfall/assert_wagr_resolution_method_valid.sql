SELECT *
FROM {{ ref('wagr_wf_green_revenue') }}
WHERE resolution_method NOT IN ('direct', 'parent', 'sector')
