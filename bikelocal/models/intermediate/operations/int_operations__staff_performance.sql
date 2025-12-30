-- KPI: Performance des employés (ventes et service)
-- Description: Agrège KPIs par employé : nombre de commandes traitées, revenus générés, clients uniques servis, AOV
-- Utilité: Alimente `dim_staff` et `fct_staff_performance` pour analyses RH et incentives
-- Colonnes clés retournées: staff_id, total_orders_processed, unique_customers_served, total_items_sold, total_sales_revenue, avg_order_value
-- Notes: Joindre à la table `stg_bike_shop__staffs` pour enrichissement (manager, store_id)
{{ config(
    materialized='view'
) }}

-- Staff performance - Performance des employés
with staff_performance as (
    select
        st.staff_id,
        st.first_name,
        st.last_name,
        st.email,
        st.phone,
        st.active,
        s.store_name,
        m.first_name as manager_first_name,
        m.last_name as manager_last_name,
        count(distinct o.order_id) as total_orders_processed,
        count(distinct o.customer_id) as unique_customers_served,
        sum(oi.quantity) as total_items_sold,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as total_sales_revenue,
        avg(oi.quantity * oi.list_price * (1 - oi.discount)) as avg_order_value,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date
    from {{ ref('stg_bike_shop__staffs') }} st
    left join {{ ref('stg_bike_shop__stores') }} s on st.store_id = s.store_id
    left join {{ ref('stg_bike_shop__staffs') }} m on st.manager_id = m.staff_id
    left join {{ ref('stg_bikelocal__orders') }} o on st.staff_id = o.staff_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on o.order_id = oi.order_id
    group by st.staff_id, st.first_name, st.last_name, st.email, st.phone, st.active,
             s.store_name, m.first_name, m.last_name
)

select * from staff_performance