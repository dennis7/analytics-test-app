SELECT *
FROM {{ ref('stg_public_market_data') }}
WHERE currency NOT IN ('USD', 'EUR', 'GBP', 'JPY')
