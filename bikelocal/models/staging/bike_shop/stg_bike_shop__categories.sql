with source as (

    select * from {{ source('bike_shop', 'categories') }}
    where category_id is not null
)

select 
    cast(category_id as Int32) as category_id,
    cast(category_name as String) as category_name
from source