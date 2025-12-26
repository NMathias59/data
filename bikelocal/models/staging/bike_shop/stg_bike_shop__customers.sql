with source as (

    select * from {{ source('bike_shop', 'customers') }}
    where customer_id is not null
)

select 
    cast(customer_id as Int32),
    cast(first_name as String),
    cast(last_name as String),
    cast(email as Nullable(String)),
    CAST(phone as Nullable(String)),
    cast(city as Nullable(String)),
    cast(state as Nullable(String)),
    cast(street as Nullable(String)),
    cast(zip_code as Nullable(String))
from source