-- KPI: Revenus par employé (KPIs financiers)
-- Description: Fournit métriques de revenus attribués aux employés pour évaluer contribution financière (orders, revenue per order, revenue rank)
-- Utilité: Sert de source pour `fct_staff_performance` et classements internes
-- Colonnes clés retournées: staff_id, total_revenue, revenue_per_order, revenue_rank_in_store
-- Notes: Metriques calculées par période; utile pour les programmes d'incitation.
{{ config(
    materialized='view'
) }}

-- Staff revenue performance - Performance des employés en termes de revenus
with staff_revenue_performance as (
    select
        st.staff_id,
        concat(st.first_name, ' ', st.last_name) as staff_full_name,
        st.email,
        st.active,
        s.store_name,
        s.city,
        s.state,
        -- Sales performance metrics
        count(distinct o.order_id) as orders_processed,
        count(distinct o.customer_id) as unique_customers_served,
        sum(oi.quantity) as total_items_sold,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as total_revenue,
        avg(oi.quantity * oi.list_price * (1 - oi.discount)) as avg_order_value,
        -- Efficiency metrics
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) / nullif(count(distinct o.order_id), 0) as revenue_per_order,
        count(distinct o.order_id) / nullif(count(distinct o.customer_id), 0) as orders_per_customer,
        -- Time-based performance
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        date_diff('day', min(o.order_date), max(o.order_date)) as days_active,
        -- Performance ranking (within store)
        dense_rank() over (
            partition by st.store_id
            order by sum(oi.quantity * oi.list_price * (1 - oi.discount)) desc
        ) as revenue_rank_in_store,
        -- Performance tier
        case
            when sum(oi.quantity * oi.list_price * (1 - oi.discount)) >= 50000 then 'Top Performer'
            when sum(oi.quantity * oi.list_price * (1 - oi.discount)) >= 30000 then 'High Performer'
            when sum(oi.quantity * oi.list_price * (1 - oi.discount)) >= 15000 then 'Good Performer'
            when sum(oi.quantity * oi.list_price * (1 - oi.discount)) >= 5000 then 'Average Performer'
            else 'Low Performer'
        end as performance_tier,
        -- Manager information
        concat(m.first_name, ' ', m.last_name) as manager_name
    from {{ ref('stg_bike_shop__staffs') }} st
    left join {{ ref('stg_bike_shop__stores') }} s on st.store_id = s.store_id
    left join {{ ref('stg_bike_shop__staffs') }} m on st.manager_id = m.staff_id
    left join {{ ref('stg_bikelocal__orders') }} o on st.staff_id = o.staff_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on o.order_id = oi.order_id
    where st.active = 1
    group by st.staff_id, st.store_id, st.first_name, st.last_name, st.email, st.active, s.store_name, s.city, s.state, m.first_name, m.last_name
)

select * from staff_revenue_performance
order by total_revenue desc