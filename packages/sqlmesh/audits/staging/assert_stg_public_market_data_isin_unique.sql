AUDIT (
    name assert_stg_public_market_data_isin_unique
);

SELECT isin, as_of_date, COUNT(*) AS cnt
FROM @this_model
GROUP BY isin, as_of_date
HAVING COUNT(*) > 1
