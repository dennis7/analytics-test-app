SELECT isin, as_of_date, COUNT(*) AS cnt
FROM {{ ref('stg_public_market_data') }}
GROUP BY isin, as_of_date
HAVING COUNT(*) > 1
