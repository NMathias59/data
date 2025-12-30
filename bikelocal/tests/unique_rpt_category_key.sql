-- Failure if any rows are returned: duplicate (year_month, category_name, price_tier, brand_name)
select year_month, category_name, price_tier, brand_name, count(*) as cnt
from {{ ref('rpt_category_growth_analysis') }}
group by year_month, category_name, price_tier, brand_name
having cnt > 1
