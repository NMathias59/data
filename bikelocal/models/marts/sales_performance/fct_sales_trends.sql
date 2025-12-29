{{ config(
    materialized='table'
) }}

WITH trends_facts AS (
    SELECT
        -- Dimensions temporelles
        concat(toString(sales_year), '-', LPAD(toString(sales_month), 2, '0')) as year_month_key,
        sales_year,
        sales_month,
        sales_quarter,
        year_month,

        -- Dimensions géographiques
        store_name,
        city,
        state,

        -- Métriques de vente depuis intermediate
        total_orders,
        unique_customers,
        total_items_sold,
        total_revenue,
        total_discounts_given,
        avg_order_value,
        prev_month_revenue,
        revenue_growth_pct,

        -- Métriques calculées avancées
        round(total_revenue / nullif(unique_customers, 0), 2) as revenue_per_customer,
        round(total_orders / nullif(unique_customers, 0), 2) as orders_per_customer,
        round(total_discounts_given / nullif(total_revenue, 0) * 100, 2) as discount_rate_pct,

        -- Classifications de performance
        CASE
            WHEN revenue_growth_pct >= 20 THEN 'Strong Growth'
            WHEN revenue_growth_pct >= 10 THEN 'Good Growth'
            WHEN revenue_growth_pct >= 0 THEN 'Stable'
            WHEN revenue_growth_pct >= -10 THEN 'Declining'
            ELSE 'Strong Decline'
        END as growth_category,

        CASE
            WHEN total_revenue >= 50000 THEN 'High Revenue'
            WHEN total_revenue >= 20000 THEN 'Medium Revenue'
            WHEN total_revenue >= 5000 THEN 'Low Revenue'
            ELSE 'Very Low Revenue'
        END as revenue_category,

        -- Métriques saisonnières
        CASE
            WHEN sales_month IN (12, 1, 2) THEN 'Winter'
            WHEN sales_month IN (3, 4, 5) THEN 'Spring'
            WHEN sales_month IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END as season,

        -- Indicateurs de tendance
        CASE
            WHEN revenue_growth_pct > 0 THEN 1
            ELSE 0
        END as is_growing,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('int_sales__trends_analysis') }}
)

SELECT
    year_month_key,
    sales_year,
    sales_month,
    sales_quarter,
    year_month,
    store_name,
    city,
    state,
    total_orders,
    unique_customers,
    total_items_sold,
    total_revenue,
    total_discounts_given,
    avg_order_value,
    prev_month_revenue,
    revenue_growth_pct,
    revenue_per_customer,
    orders_per_customer,
    discount_rate_pct,
    growth_category,
    revenue_category,
    season,
    is_growing,
    created_at,
    created_by
FROM trends_facts
ORDER BY sales_year DESC, sales_month DESC, total_revenue DESC