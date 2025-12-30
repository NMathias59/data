-- KPI: Performance magasin (ventes et clientèle par magasin)
-- Description: Calcule des métriques par magasin (commandes, revenus, clients uniques, AOV) pour analyser la performance par emplacement.
-- Utilité: Sert d'entrée pour `fct_sales` et tableaux de bord par magasin
-- Colonnes clés retournées: store_id, store_name, total_orders, unique_customers, total_revenue, avg_order_value
-- Notes: Agrégation par magasin et période; utile pour la décision opérationnelle locale.
{{ config(
    materialized='incremental',
    unique_key='store_id'
) }}

-- Sales by store - Ventes par magasin
with store_sales as (
    select
        s.store_id as store_id,
        s.store_name as store_name,
        s.city as city,
        s.state as state,
        s.street as street,
        s.zip_code as zip_code,
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as unique_customers,
        sum(oi.quantity) as total_items_sold,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as total_revenue,
        avg(oi.quantity * oi.list_price * (1 - oi.discount)) as avg_order_value,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date
    from {{ ref('stg_bike_shop__stores') }} s
    left join {{ ref('stg_bikelocal__orders') }} o on s.store_id = o.store_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on o.order_id = oi.order_id
    group by s.store_id, s.store_name, s.city, s.state, s.street, s.zip_code
)

select * from store_sales