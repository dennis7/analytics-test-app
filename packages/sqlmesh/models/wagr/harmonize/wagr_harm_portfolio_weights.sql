MODEL (
    name wagr.harm_portfolio_weights,
    kind FULL,
    audits (assert_wagr_harm_weights_not_null),
    description 'Harmonized positions with portfolio weights'
);

@SHARED_HARM_PORTFOLIO_WEIGHTS('wagr')
