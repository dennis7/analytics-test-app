with source as (

    select * from {{ read_parquet('portfolio_positions') }}

)

select
    portfolio_id::varchar as portfolio_id,
    security_id::varchar as security_id,
    quantity::double as quantity,
    as_of_date::date as as_of_date

from source
