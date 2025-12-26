with source as (
    select * from {{ source('bike_shop', 'order_items') }}
    where item_id is not null
)

SELECT
    cast(item_id as Int32),
    cast(order_id as Int32),
    cast(product_id as Int32),
    cast(discount as Float64),
    cast(quantity as Int32),
    cast(list_price as Float64)
FROM source