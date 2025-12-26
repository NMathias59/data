
with sources as (

    select * from {{ source('bike_shop', 'brands') }}

)

select 
    cast(brand_id as int),
    cast(brand_name as String)
from sources