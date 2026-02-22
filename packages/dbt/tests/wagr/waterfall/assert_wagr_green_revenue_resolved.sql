SELECT *
FROM {{ ref('wagr_wf_green_revenue') }}
WHERE resolved_green_revenue_share IS NULL
