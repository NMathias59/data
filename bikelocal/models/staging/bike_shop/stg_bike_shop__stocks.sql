with source as (

    select * from {{ source('bike_shop', 'stocks') }}
)

select 
    cast(store_id as Int32),
    cast(product_id as Int32),
    cast(quantity as Int32)
from source