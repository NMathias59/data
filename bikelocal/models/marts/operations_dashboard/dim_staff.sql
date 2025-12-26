{{ config(
    materialized='table',
    
) }}

WITH staff_data AS (
    SELECT
        s.staff_id,
        s.first_name,
        s.last_name,
        concat(s.first_name, ' ', s.last_name) as full_name,
        s.email,
        s.phone,
        s.active,
        -- Manager information
        m.first_name as manager_first_name,
        m.last_name as manager_last_name,
        concat(m.first_name, ' ', m.last_name) as manager_full_name,
        -- Store information
        st.store_name,
        st.city as store_city,
        st.state as store_state,
        st.region as store_region,
        -- Performance metrics from intermediate
        sp.total_orders_processed,
        sp.unique_customers_served,
        sp.total_items_sold,
        sp.total_sales_revenue,
        sp.avg_order_value,
        sp.first_order_date,
        sp.last_order_date,
        -- Performance tier
        spr.performance_tier,
        spr.revenue_rank_in_store
    FROM {{ ref('stg_bike_shop__staffs') }} s
    LEFT JOIN {{ ref('stg_bike_shop__staffs') }} m ON s.manager_id = m.staff_id
    LEFT JOIN {{ ref('stg_bike_shop__stores') }} st ON s.store_id = st.store_id
    LEFT JOIN {{ ref('int_operations__staff_performance') }} sp ON s.staff_id = sp.staff_id
    LEFT JOIN {{ ref('int_operations__staff_revenue_performance') }} spr ON s.staff_id = spr.staff_id
)

SELECT
    staff_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    active,
    manager_first_name,
    manager_last_name,
    manager_full_name,
    store_name,
    store_city,
    store_state,
    store_region,
    total_orders_processed,
    unique_customers_served,
    total_items_sold,
    total_sales_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    performance_tier,
    revenue_rank_in_store,
    -- Métadonnées
    now() as created_at,
    'dbt' as created_by
FROM staff_data
ORDER BY staff_id
