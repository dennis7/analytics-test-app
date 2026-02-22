WITH public_securities AS (
    SELECT * FROM {metric}.prep_public_securities
),

private_securities AS (
    SELECT * FROM {metric}.prep_private_securities
),

market_data AS (
    SELECT * FROM staging.stg_public_market_data
),

private_valuations AS (
    SELECT * FROM staging.stg_private_valuations
),

valued_public AS (
    SELECT
        ps.portfolio_id,
        ps.internal_id,
        ps.security_type,
        ps.sector,
        ps.entity_id,
        (ps.quantity * md.price)::DOUBLE AS market_value,
        ps.as_of_date
    FROM public_securities ps
    INNER JOIN market_data md
        ON ps.isin = md.isin
        AND ps.as_of_date = md.as_of_date
),

valued_private AS (
    SELECT
        ps.portfolio_id,
        ps.internal_id,
        ps.security_type,
        ps.sector,
        ps.entity_id,
        (ps.units_held * pv.nav_per_unit)::DOUBLE AS market_value,
        ps.as_of_date
    FROM private_securities ps
    INNER JOIN private_valuations pv
        ON ps.fund_id = pv.fund_id
        AND ps.as_of_date = pv.as_of_date
)

SELECT * FROM valued_public
UNION ALL
SELECT * FROM valued_private
