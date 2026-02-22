SELECT *
FROM {{ ref('waci_harm_valued_positions') }}
WHERE market_value <= 0
