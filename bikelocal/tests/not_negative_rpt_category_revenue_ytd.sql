-- Failure if any rows are returned: revenue_ytd must be >= 0
select *
from {{ ref('rpt_category_growth_analysis') }}
where revenue_ytd < 0
