with source as (
    select * from {{ source('bike_shop', 'orders') }}
    where order_id is not null
)

select 
    cast(order_id as Int32) as order_id,
    cast(customer_id as Int32) as customer_id,
    cast(staff_id as Int32) as staff_id,
    cast(store_id as Int32) as store_id,
    cast(order_status as String) as order_status,
    cast(order_date as DateTime) as order_date,
    cast(required_date as Nullable(DateTime)) as required_date,
    cast(shipped_date as Nullable(DateTime)) as shipped_date
    
from source
