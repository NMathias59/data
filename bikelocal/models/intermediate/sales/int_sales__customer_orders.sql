{{ config(
    materialized='view'
) }}

-- Customer orders summary - Résumé des commandes par client
with customer_orders as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.city,
        c.state,
        count(distinct o.order_id) as total_orders,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as total_amount,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        avg(oi.quantity * oi.list_price * (1 - oi.discount)) as avg_order_value
    from {{ ref('stg_bike_shop__customers') }} c
    left join {{ ref('stg_bikelocal__orders') }} o on c.customer_id = o.customer_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name, c.email, c.phone, c.city, c.state
)

select * from customer_orders