-- KPI: Performance opérationnelle (fulfillment & staff metrics)
-- Description: Table de faits synthétisant qualité de fulfillment (on-time rate, days_to_ship) et métriques par employé (orders processed, revenue)
-- Utilité: Permet de suivre SLA, productivité staff et impact sur l'expérience client; utilisée pour dashboards d'opérations
-- Colonnes clés retournées: order_id, customer_id, staff_id, store_name, order_date, fulfillment_status, days_to_ship, total_orders_processed, total_sales_revenue, performance_tier
-- Notes: Agrégat au niveau commande avec métriques staff dénormalisées pour faciliter reporting; vérifier fréquences et granularité.
{{ config(
    materialized='table'
) }}

SELECT
    -- Dimensions commande (from intermediate)
    of.order_id,
    of.customer_id,
    of.staff_id,
    of.store_name,
    of.order_date,
    of.required_date,
    of.shipped_date,
    of.order_status,
    of.total_items as total_items,
    of.order_total as order_total,

    -- Métriques de fulfillment
    of.fulfillment_status as fulfillment_status,
    of.days_to_ship as days_to_ship,
    of.days_past_due as days_past_due,

    -- Métriques staff (from int staff performance)
    sp.total_orders_processed as total_orders_processed,
    sp.unique_customers_served as unique_customers_served,
    sp.total_items_sold as total_items_sold,
    sp.total_sales_revenue as total_sales_revenue,
    sp.avg_order_value as avg_order_value,
    sp.first_order_date as spr_first_order,
    sp.last_order_date as spr_last_order,

    -- Métriques staff revenue (from staff revenue performance int)
    srp.orders_processed as spr_orders_processed,
    srp.unique_customers_served as spr_unique_customers_served,
    srp.total_items_sold as spr_total_items_sold,
    srp.total_revenue as spr_total_revenue,
    srp.avg_order_value as spr_avg_order_value,
    srp.revenue_per_order as spr_revenue_per_order,
    srp.orders_per_customer as spr_orders_per_customer,
    srp.days_active as spr_days_active,
    srp.revenue_rank_in_store as spr_revenue_rank_in_store,
    srp.performance_tier as spr_performance_tier,
    srp.manager_name as spr_manager_name,

    -- Classifications opérationnelles derived
    CASE
        WHEN of.fulfillment_status = 'On Time' THEN 'Excellent'
        WHEN of.fulfillment_status = 'Late' THEN 'Needs Improvement'
        WHEN of.fulfillment_status = 'Overdue' THEN 'Critical'
        ELSE 'Pending'
    END as fulfillment_performance,

    CASE
        WHEN srp.performance_tier = 'Top Performer' THEN 'High Performer'
        WHEN srp.performance_tier = 'High Performer' THEN 'Good Performer'
        WHEN srp.performance_tier IN ('Good Performer', 'Average Performer') THEN 'Average Performer'
        ELSE 'Low Performer'
    END as staff_performance_category,

    -- Métriques de qualité de service
    CASE
        WHEN of.days_to_ship <= 1 THEN 'Very Fast'
        WHEN of.days_to_ship <= 3 THEN 'Fast'
        WHEN of.days_to_ship <= 7 THEN 'Normal'
        ELSE 'Slow'
    END as shipping_speed_category,

    -- Métadonnées
    now() as created_at,
    'dbt' as created_by

FROM {{ ref('int_operations__order_fulfillment') }} of
LEFT JOIN {{ ref('int_operations__staff_performance') }} sp ON of.staff_id = sp.staff_id
LEFT JOIN {{ ref('int_operations__staff_revenue_performance') }} srp ON of.staff_id = srp.staff_id