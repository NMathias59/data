with source as 
(

    select * from {{ source('bike_shop', 'staffs') }}
    WHERE staff_id IS NOT NULL
)

select 
    cast(staff_id as Int32) as staff_id,
    cast(store_id as Int32) as store_id,
    cast(manager_id as Nullable(Int32)) as manager_id,
    cast(first_name as String) as first_name,
    cast(last_name as String) as last_name,
    cast(email as Nullable(String)) as email,
    cast(phone as Nullable(String)) as phone,
    cast(active as Int8) as active
from source