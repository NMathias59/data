{{ config(
    materialized='table',
    
) }}

WITH staff_performance_facts AS (
    SELECT
        -- Dimensions de base
        o.staff_id,
        o.store_id,
        o.order_date as performance_date_key,

        -- Métriques de performance
        1 as orders_count,
        o.customer_id,
        oi.quantity as items_sold,
        (oi.list_price * oi.quantity * (1 - oi.discount)) as revenue_generated,

        -- Métriques temporelles
        toYear(o.order_date) as performance_year,
        toMonth(o.order_date) as performance_month,
        formatDateTime(o.order_date, '%Y-%m') as performance_year_month,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('stg_bikelocal__orders') }} o
    JOIN {{ ref('stg_bikelocal__order_items') }} oi ON o.order_id = oi.order_id
)

SELECT
    staff_id,
    store_id,
    performance_date_key,
    orders_count,
    customer_id,
    items_sold,
    revenue_generated,
    performance_year,
    performance_month,
    performance_year_month,
    created_at,
    created_by
FROM staff_performance_facts
ORDER BY staff_id, performance_date_key
