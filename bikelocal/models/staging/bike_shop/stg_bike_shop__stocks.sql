with source as (

    select * from {{ source('bike_shop', 'stocks') }}
)

select 
    cast(store_id as int),
    cast(product_id as int),
    cast(quantity as int)
from source