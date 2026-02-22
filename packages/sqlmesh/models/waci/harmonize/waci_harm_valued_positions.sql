MODEL (
    name waci.harm_valued_positions,
    kind FULL,
    audits (assert_waci_harm_market_value_positive),
    description 'Unified public and private positions with market values'
);

@SHARED_HARM_VALUED_POSITIONS('waci')
