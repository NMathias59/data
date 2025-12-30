-- KPI: Performance commerciale par employé
-- Description: Agrège indicateurs de vente par employé (commandes, revenu, AOV) et fournit des métriques de ranking
-- Utilité: Sert pour incentive, suivi des objectifs et détection des outliers (top/bottom performers)
-- Colonnes clés retournées: staff_id, total_orders_processed, total_sales_revenue, avg_order_value, revenue_rank_in_store, performance_tier
-- Notes: Inclut métriques temporelles; utile pour créer targets et alertes RH.
{{ config(
    materialized='table'
) }}


WITH staff_performance_facts AS (
    SELECT
        sp.staff_id,
        sp.store_name,
        sp.total_orders_processed as total_orders_processed,
        sp.unique_customers_served as unique_customers_served,
        sp.total_items_sold as total_items_sold,
        sp.total_sales_revenue as total_sales_revenue,
        sp.avg_order_value as avg_order_value,
        sp.first_order_date as first_order_date,
        sp.last_order_date as last_order_date,
        now() as created_at,
        'dbt' as created_by
    FROM {{ ref('int_operations__staff_performance') }} sp
)

SELECT
    staff_id,
    store_name,
    total_orders_processed,
    unique_customers_served,
    total_items_sold,
    total_sales_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    created_at,
    created_by
FROM staff_performance_facts
ORDER BY staff_id
