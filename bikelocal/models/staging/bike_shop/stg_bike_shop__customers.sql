with source as (

    select * from {{ source('bike_shop', 'customers') }}
    where customer_id is not null
)

select 
    cast(customer_id as int),
    cast(first_name as String),
    cast(last_name as String),
    cast(email as String),
    CAST(phone as String),
    cast(city as String),
    cast(state as String),
    cast(street as String),
    cast(zip_code as String)
from source