-- Relationship test: category_name in rpt should exist in dim_products
select *
from {{ ref('rpt_category_growth_analysis') }} r
left join {{ ref('dim_products') }} p
  on r.category_name = p.category_name
where p.category_name is null
