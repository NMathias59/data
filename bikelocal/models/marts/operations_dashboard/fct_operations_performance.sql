{{ config(
    materialized='table'
) }}

WITH operations_facts AS (
    SELECT
        -- Dimensions commande
        of.order_id,
        of.customer_id,
        of.staff_id,
        of.store_name,
        of.order_date,
        of.required_date,
        of.shipped_date,
        of.order_status,
        of.total_items,
        of.order_total,

        -- Métriques de fulfillment
        of.fulfillment_status,
        of.days_to_ship,
        of.days_past_due,

        -- Métriques staff depuis intermediate
        sp.total_orders_processed,
        sp.unique_customers_served,
        sp.total_items_sold,
        sp.total_sales_revenue,
        sp.avg_order_value,
        sp.first_order_date,
        sp.last_order_date,

        -- Métriques staff ranking
        spr.orders_processed,
        spr.unique_customers_served,
        spr.total_items_sold,
        spr.total_revenue,
        spr.avg_order_value as staff_avg_order_value,
        spr.revenue_per_order,
        spr.orders_per_customer,
        spr.first_order_date as staff_first_order,
        spr.last_order_date as staff_last_order,
        spr.days_active,
        spr.revenue_rank_in_store,
        spr.performance_tier,
        spr.manager_name,

        -- Classifications opérationnelles
        CASE
            WHEN of.fulfillment_status = 'On Time' THEN 'Excellent'
            WHEN of.fulfillment_status = 'Late' THEN 'Needs Improvement'
            WHEN of.fulfillment_status = 'Overdue' THEN 'Critical'
            ELSE 'Pending'
        END as fulfillment_performance,

        CASE
            WHEN spr.performance_tier = 'Top' THEN 'High Performer'
            WHEN spr.performance_tier = 'High' THEN 'Good Performer'
            WHEN spr.performance_tier IN ('Good', 'Average') THEN 'Average Performer'
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
    LEFT JOIN {{ ref('int_operations__staff_revenue_performance') }} spr ON of.staff_id = spr.staff_id
)

SELECT
    order_id,
    customer_id,
    staff_id,
    store_name,
    order_date,
    required_date,
    shipped_date,
    order_status,
    total_items,
    order_total,
    fulfillment_status,
    days_to_ship,
    days_past_due,
    total_orders_processed,
    unique_customers_served,
    total_items_sold,
    total_sales_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    orders_processed,
    revenue_per_order,
    orders_per_customer,
    staff_first_order,
    staff_last_order,
    days_active,
    revenue_rank_in_store,
    performance_tier,
    manager_name,
    fulfillment_performance,
    staff_performance_category,
    shipping_speed_category,
    created_at,
    created_by
FROM operations_facts
ORDER BY order_date DESC, order_id DESC