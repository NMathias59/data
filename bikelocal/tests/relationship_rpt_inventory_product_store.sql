-- Relationship test: rpt_inventory_status product and store should match dims
select r.*
from {{ ref('rpt_inventory_status') }} r
left join {{ ref('dim_products') }} p on r.product_name = p.product_name
left join {{ ref('dim_stores') }} s on r.store_name = s.store_name
where p.product_id is null or s.store_id is null
