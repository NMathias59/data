with source as (

    select * from {{ source('bike_shop', 'products') }}
    where product_id is not null
)

select 
    cast(product_id as Int32),
    cast(brand_id as Int32),
    cast(category_id as Int32),
    cast(product_name as String),
    cast(model_year as Int32),
    cast(list_price as Float64)
from source