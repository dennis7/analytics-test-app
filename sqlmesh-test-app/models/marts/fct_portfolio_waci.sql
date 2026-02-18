MODEL (
    name marts.fct_portfolio_waci,
    kind FULL,
    audits (
        assert_fct_portfolio_waci_not_null,
        assert_fct_portfolio_waci_unique,
        assert_waci_bounds
    ),
    description 'Weighted Average Carbon Intensity (WACI) per portfolio'
);

WITH portfolio_carbon AS (

    SELECT * FROM intermediate.int_portfolio_carbon

)

SELECT
    portfolio_id,
    as_of_date,
    SUM(weighted_carbon_intensity)::DOUBLE AS waci,
    SUM(market_value)::DOUBLE AS total_market_value,
    COUNT(*)::BIGINT AS num_holdings

FROM portfolio_carbon
GROUP BY portfolio_id, as_of_date
