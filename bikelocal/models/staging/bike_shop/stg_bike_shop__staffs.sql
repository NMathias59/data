with source as 
(

    select * from {{ source('bike_shop', 'staffs') }}
    WHERE staff_id IS NOT NULL
)

select 
    cast(staff_id as Int32),
    cast(store_id as Int32),
    cast(manager_id as Nullable(Int32)),
    cast(first_name as String),
    cast(last_name as String),
    cast(email as Nullable(String)),
    cast(phone as Nullable(String)),
    cast(active as Int8)
from source