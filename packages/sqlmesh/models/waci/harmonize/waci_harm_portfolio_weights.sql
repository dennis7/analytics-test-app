MODEL (
    name waci.harm_portfolio_weights,
    kind FULL,
    audits (assert_waci_harm_weights_not_null),
    description 'Harmonized positions with portfolio weights'
);

@SHARED_HARM_PORTFOLIO_WEIGHTS('waci')
