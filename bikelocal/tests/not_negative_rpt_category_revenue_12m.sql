-- Failure if any rows are returned: revenue_12m_rolling_avg must be >= 0
select *
from {{ ref('rpt_category_growth_analysis') }}
where revenue_12m_rolling_avg < 0
