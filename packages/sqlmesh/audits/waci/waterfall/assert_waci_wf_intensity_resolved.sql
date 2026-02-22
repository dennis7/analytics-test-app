AUDIT (
    name assert_waci_wf_intensity_resolved
);

SELECT *
FROM @this_model
WHERE resolved_carbon_intensity IS NULL
