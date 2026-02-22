AUDIT (
    name assert_stg_private_valuations_method_valid
);

SELECT *
FROM @this_model
WHERE valuation_method NOT IN ('appraisal', 'transaction')
