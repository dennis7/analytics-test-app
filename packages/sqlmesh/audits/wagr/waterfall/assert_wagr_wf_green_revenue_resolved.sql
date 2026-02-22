AUDIT (
    name assert_wagr_wf_green_revenue_resolved
);

SELECT *
FROM @this_model
WHERE resolved_green_revenue_share IS NULL
