-- Failure if any rows are returned: duplicate customer_id in rpt_customer_ltv
select customer_id, count(*) as cnt
from {{ ref('rpt_customer_ltv') }}
group by customer_id
having cnt > 1
