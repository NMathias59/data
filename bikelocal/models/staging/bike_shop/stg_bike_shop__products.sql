with source as (

    select * from {{ source('bike_shop', 'products') }}
    where product_id is not null
)

select 
    cast(product_id as Int32) as product_id,
    cast(brand_id as Int32) as brand_id,
    cast(category_id as Int32) as category_id,
    cast(product_name as String) as product_name,
    cast(model_year as Int32) as model_year,
    cast(list_price as Float64) as list_price
from source