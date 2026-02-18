MODEL (
    name intermediate.int_portfolio_carbon,
    kind FULL,
    audits (assert_int_portfolio_carbon_not_null),
    description 'Portfolio exposure combined with carbon intensity data'
);

WITH exposure AS (

    SELECT * FROM intermediate.int_portfolio_exposure

),

carbon AS (

    SELECT * FROM staging.stg_carbon_scores

)

SELECT
    e.portfolio_id,
    e.security_id,
    e.quantity,
    e.price,
    e.market_value,
    e.portfolio_weight,
    c.company_name,
    c.carbon_emissions_tonnes,
    c.revenue_usd,
    c.carbon_intensity,
    (e.portfolio_weight * c.carbon_intensity)::DOUBLE AS weighted_carbon_intensity,
    e.as_of_date

FROM exposure e
INNER JOIN carbon c
    ON e.security_id = c.security_id
    AND e.as_of_date = c.as_of_date
