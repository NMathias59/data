
with sources as (

    select * from {{ source('bike_shop', 'brands') }}

)

select 
    cast(brand_id as Int32) as brand_id,
    cast(brand_name as String) as brand_name
from sources