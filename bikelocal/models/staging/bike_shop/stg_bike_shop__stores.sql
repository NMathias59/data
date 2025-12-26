with source as (

    select * from {{ source('bike_shop', 'stores') }}
    where store_id is not null
)

select
    cast(store_id as Int32) as store_id,
    cast(store_name as String) as store_name,
    cast(city as Nullable(String)) as city,
    cast(state as Nullable(String)) as state,
    cast(street as Nullable(String)) as street,
    cast(zip_code as Nullable(String)) as zip_code,
    cast(phone as Nullable(String)) as phone,
    cast(email as Nullable(String)) as email
from source
