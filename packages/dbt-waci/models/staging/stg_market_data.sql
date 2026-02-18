with source as (

    select * from {{ read_parquet('market_data') }}

)

select
    security_id::varchar as security_id,
    price::double as price,
    currency::varchar as currency,
    as_of_date::date as as_of_date

from source
