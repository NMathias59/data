-- KPI: Commandes et comportements clients (agrégat par client)
-- Description: Résume les commandes par client (nombre de commandes, fréquence, valeur moyenne) et retourne des métriques RFM partielles.
-- Utilité: Base pour `dim_customers` (RFM/LTV) et analyses de rétention
-- Colonnes clés retournées: customer_id, total_orders, first_order_date, last_order_date, avg_order_value, lifetime_value, order_frequency
-- Notes: Agrégation par client; utiliser coalesce et nullif pour éviter les divisions par zéro.
{{ config(
    materialized='view'
) }}

-- Customer orders summary - Résumé des commandes par client pour analyse RFM
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
        avg(oi.quantity * oi.list_price * (1 - oi.discount)) as avg_order_value,
        -- RFM Analysis components
        date_diff('day', max(o.order_date), today()) as days_since_last_order,
        count(distinct o.order_date) as order_frequency,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as monetary_value,
        -- Customer segmentation
        case
            when date_diff('day', max(o.order_date), today()) <= 30 then 'Active'
            when date_diff('day', max(o.order_date), today()) <= 90 then 'Recent'
            when date_diff('day', max(o.order_date), today()) <= 180 then 'At Risk'
            else 'Lost'
        end as customer_segment
    from {{ ref('stg_bike_shop__customers') }} c
    left join {{ ref('stg_bikelocal__orders') }} o on c.customer_id = o.customer_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on o.order_id = oi.order_id
    group by c.customer_id, c.first_name, c.last_name, c.email, c.phone, c.city, c.state
)

select * from customer_orders