with source as (
    select * from {{ source('bike_shop', 'order_items') }}
    where item_id is not null
)

SELECT
    cast(item_id as Int32) as item_id,
    cast(order_id as Int32) as order_id,
    cast(product_id as Int32) as product_id,
    cast(discount as Float64) as discount,
    cast(quantity as Int32) as quantity,
    cast(list_price as Float64) as list_price
FROM source