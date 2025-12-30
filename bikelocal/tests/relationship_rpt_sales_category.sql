-- Relationship test: category_name and brand_name in rpt_sales_summary should exist in dim_products
select r.*
from {{ ref('rpt_sales_summary') }} r
left join {{ ref('dim_products') }} p
  on r.category_name = p.category_name and r.brand_name = p.brand_name
where p.product_id is null
