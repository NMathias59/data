with source as (

    select * from {{ source('bike_shop', 'customers') }}
    where customer_id is not null
)

select 
    cast(customer_id as Int32) as customer_id,
    cast(first_name as String) as first_name,
    cast(last_name as String) as last_name,
    cast(email as Nullable(String)) as email,
    cast(phone as Nullable(String)) as phone,
    cast(city as Nullable(String)) as city,
    cast(state as Nullable(String)) as state,
    cast(street as Nullable(String)) as street,
    cast(zip_code as Nullable(String)) as zip_code
from source