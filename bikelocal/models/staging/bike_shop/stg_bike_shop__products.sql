with source as (

    select * from {{ source('bike_shop', 'products') }}
    where product_id is not null
)

select 
    cast(product_id as int),
    cast(brand_id as int),
    cast(category_id as int),
    cast(product_name as String),
    cast(model_year as int),
    cast(list_price as FLOAT)
from source