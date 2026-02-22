MODEL (
    name wagr.harm_valued_positions,
    kind FULL,
    audits (assert_wagr_harm_market_value_positive),
    description 'Unified public and private positions with market values'
);

@SHARED_HARM_VALUED_POSITIONS('wagr')
