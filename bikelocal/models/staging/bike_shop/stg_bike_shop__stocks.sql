with source as (

    select * from {{ source('bike_shop', 'stocks') }}
)

select 
    cast(store_id as Int32) as store_id,
    cast(product_id as Int32) as product_id,
    cast(quantity as Int32) as quantity
from source