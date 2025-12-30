-- Failure if any rows are returned: any net_revenue over a high sanity threshold (100M)
select year_month, net_revenue
from {{ ref('rpt_sales_summary') }}
where net_revenue > 100000000
