with source as 
(

    select * from {{ source('bike_shop', 'staffs') }}
    WHERE staff_id IS NOT NULL
)

select 
    cast(staff_id as int),
    cast(store_id as int),
    cast(manager_id as int),
    cast(first_name as String),
    cast(last_name as String),
    cast(email as String),
    cast(phone as String),
    cast(active as int)
from source