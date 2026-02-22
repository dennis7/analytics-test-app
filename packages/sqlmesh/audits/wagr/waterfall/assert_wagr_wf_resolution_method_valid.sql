AUDIT (
    name assert_wagr_wf_resolution_method_valid
);

SELECT *
FROM @this_model
WHERE resolution_method NOT IN ('direct', 'parent', 'sector')
