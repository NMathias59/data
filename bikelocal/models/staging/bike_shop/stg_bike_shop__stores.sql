with source as (

    select * from {{ source('bike_shop', 'stores') }}
    where store_id is not null
)

select
    cast(store_id as int),
    cast(store_name as String),
    cast(city as String),
    cast(state as String),
    cast(street as String),
    cast(zip_code as String),
    cast(phone as String),
    cast(email as String)
from source
