-- Failure if any rows are returned: duplicate (store_name, product_name)
select store_name, product_name, count(*) as cnt
from {{ ref('rpt_inventory_status') }}
group by store_name, product_name
having cnt > 1
