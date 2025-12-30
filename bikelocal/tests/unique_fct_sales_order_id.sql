-- Failure if any rows are returned: duplicate order_id in fct_sales
select order_id, count(*) as cnt
from {{ ref('fct_sales') }}
group by order_id
having cnt > 1
