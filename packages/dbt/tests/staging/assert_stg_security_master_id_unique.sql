SELECT internal_id, COUNT(*) AS cnt
FROM {{ ref('stg_security_master') }}
GROUP BY internal_id
HAVING COUNT(*) > 1
