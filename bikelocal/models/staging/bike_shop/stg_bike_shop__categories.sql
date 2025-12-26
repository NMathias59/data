with source as (

    select * from {{ source('bike_shop', 'categories') }}
    where category_id is not null
)

select 
    cast(category_id as int),
    cast(category_name as String)
from source