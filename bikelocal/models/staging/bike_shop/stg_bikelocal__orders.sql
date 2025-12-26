with source as (
    select * from {{ source('bike_shop', 'orders') }}
    where order_id is not null
)

select 
    cast(order_id as Int32),
    cast(customer_id as Int32),
    cast(staff_id as Int32),
    cast(store_id as Int32),
    cast(order_status as String),
    cast(order_date as DateTime),
    cast(required_date as Nullable(DateTime)),
    cast(shipped_date as Nullable(DateTime))
    
from source
