with source as (
    select * from {{ source('bike_shop', 'orders') }}
    where order_id is not null
)

select 
    cast(order_id as int),
    cast(customer_id as int),
    cast(staff_id as int),
    cast(store_id as int),
    cast(order_status as String),
    cast(order_date as timestamp),
    cast(required_date as timestamp),
    cast(shipped_date as timestamp)
    
from source
