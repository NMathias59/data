{{ config(
    materialized='table'
) }}

SELECT
    -- Dimensions commande
    o.order_id,
    o.customer_id,
    o.staff_id,
    st.store_name,
    o.order_date,
    o.required_date,
    o.shipped_date,
    o.order_status,
    0 as total_items,
    0 as order_total,

    -- Métriques de fulfillment
    'Unknown' as fulfillment_status,
    0 as days_to_ship,
    0 as days_past_due,

    -- Métriques staff (valeurs par défaut)
    0 as total_orders_processed,
    0 as unique_customers_served,
    0 as total_items_sold,
    0 as total_sales_revenue,
    0 as avg_order_value,
    '1900-01-01' as spr_first_order,
    '1900-01-01' as spr_last_order,

    -- Métriques staff revenue (valeurs par défaut)
    0 as spr_orders_processed,
    0 as spr_unique_customers_served,
    0 as spr_total_items_sold,
    0 as spr_total_revenue,
    0 as spr_avg_order_value,
    0 as spr_revenue_per_order,
    0 as spr_orders_per_customer,
    0 as spr_days_active,
    0 as spr_revenue_rank_in_store,
    'Unknown' as spr_performance_tier,
    'Unknown' as spr_manager_name,

    -- Classifications opérationnelles
    CASE
        WHEN 'Unknown' = 'On Time' THEN 'Excellent'
        WHEN 'Unknown' = 'Late' THEN 'Needs Improvement'
        WHEN 'Unknown' = 'Overdue' THEN 'Critical'
        ELSE 'Pending'
    END as fulfillment_performance,

    CASE
        WHEN 'Unknown' = 'Top Performer' THEN 'High Performer'
        WHEN 'Unknown' = 'High Performer' THEN 'Good Performer'
        WHEN 'Unknown' IN ('Good Performer', 'Average Performer') THEN 'Average Performer'
        ELSE 'Low Performer'
    END as staff_performance_category,

    -- Métriques de qualité de service
    CASE
        WHEN 0 <= 1 THEN 'Very Fast'
        WHEN 0 <= 3 THEN 'Fast'
        WHEN 0 <= 7 THEN 'Normal'
        ELSE 'Slow'
    END as shipping_speed_category,

    -- Métadonnées
    now() as created_at,
    'dbt' as created_by

FROM {{ ref('stg_bikelocal__orders') }} o
LEFT JOIN {{ ref('stg_bike_shop__customers') }} c ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('stg_bike_shop__staffs') }} s ON o.staff_id = s.staff_id
LEFT JOIN {{ ref('stg_bike_shop__stores') }} st ON o.store_id = st.store_id