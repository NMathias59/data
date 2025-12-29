{{ config(
    materialized='table',
    
) }}

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
    -- Performance metrics (valeurs par défaut pour le moment)
    0 as total_orders_processed,
    0 as unique_customers_served,
    0 as total_items_sold,
    0 as total_sales_revenue,
    0 as avg_order_value,
    '1900-01-01' as first_order_date,
    '1900-01-01' as last_order_date,
    -- Performance tier
    'Unknown' as performance_tier,
    0 as revenue_rank_in_store,
    -- Métadonnées
    now() as created_at,
    'dbt' as created_by
FROM {{ ref('stg_bike_shop__staffs') }} s
LEFT JOIN {{ ref('stg_bike_shop__staffs') }} m ON s.manager_id = m.staff_id
LEFT JOIN {{ ref('stg_bike_shop__stores') }} st ON s.store_id = st.store_id
