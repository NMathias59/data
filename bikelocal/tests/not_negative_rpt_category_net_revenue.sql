-- Failure if any rows are returned: net_revenue must be >= 0
select *
from {{ ref('rpt_category_growth_analysis') }}
where net_revenue < 0
