with source as (

    select * from {{ source('bike_shop', 'stores') }}
    where store_id is not null
)

select
    cast(store_id as Int32),
    cast(store_name as String),
    cast(city as Nullable(String)),
    cast(state as Nullable(String)),
    cast(street as Nullable(String)),
    cast(zip_code as Nullable(String)),
    cast(phone as Nullable(String)),
    cast(email as Nullable(String))
from source
