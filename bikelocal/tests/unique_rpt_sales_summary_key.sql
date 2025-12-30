-- Failure if any rows are returned: duplicate (year_month, category_name, price_tier)
select year_month, category_name, price_tier, count(*) as cnt
from {{ ref('rpt_sales_summary') }}
group by year_month, category_name, price_tier
having cnt > 1
