AUDIT (
    name assert_stg_market_data_currency_valid
);

SELECT *
FROM @this_model
WHERE currency NOT IN ('USD', 'EUR', 'GBP', 'JPY')
