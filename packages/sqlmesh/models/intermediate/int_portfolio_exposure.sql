MODEL (
    name intermediate.int_portfolio_exposure,
    kind FULL,
    audits (assert_int_portfolio_exposure_not_null),
    description 'Portfolio positions enriched with market values and portfolio weights'
);

WITH positions AS (

    SELECT * FROM staging.stg_portfolio_positions

),

market_data AS (

    SELECT * FROM staging.stg_market_data

),

valued_positions AS (

    SELECT
        p.portfolio_id,
        p.security_id,
        p.quantity,
        m.price,
        m.currency,
        (p.quantity * m.price)::DOUBLE AS market_value,
        p.as_of_date

    FROM positions p
    INNER JOIN market_data m
        ON p.security_id = m.security_id
        AND p.as_of_date = m.as_of_date

)

SELECT
    portfolio_id,
    security_id,
    quantity,
    price,
    currency,
    market_value,
    (market_value / SUM(market_value) OVER (
        PARTITION BY portfolio_id, as_of_date
    ))::DOUBLE AS portfolio_weight,
    as_of_date

FROM valued_positions
