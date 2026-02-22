SELECT *
FROM {{ ref('stg_security_master') }}
WHERE security_type NOT IN ('equity', 'bond', 'private_equity')
