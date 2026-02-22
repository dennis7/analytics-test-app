SELECT *
FROM {{ ref('wagr_harm_valued_positions') }}
WHERE market_value <= 0
