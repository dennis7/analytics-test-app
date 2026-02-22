SELECT *
FROM {{ ref('stg_private_valuations') }}
WHERE valuation_method NOT IN ('appraisal', 'transaction')
