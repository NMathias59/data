with source as (
    select * from {{ source('bike_shop', 'order_items') }}
    where item_id is not null
)

SELECT
    cast(item_id as int),
    cast(order_id as int),
    cast(product_id as int),
    cast(discount as FLOAT),
    cast(quantity as int),
    cast(list_price as FLOAT)
FROM source