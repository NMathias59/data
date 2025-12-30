-- KPI: Tendances de ventes (périodicité mensuelle/trimestrielle)
-- Description: Regroupe et calcule les indicateurs de tendance (croissance, prev_month_revenue, pct growth) par période et découpage géographique/produit.
-- Utilité: Alimente `fct_sales_trends` et rapports de suivi de la performance temporelle
-- Colonnes clés retournées: year, month, year_month, total_revenue, prev_month_revenue, revenue_growth_pct, avg_order_value
-- Notes: Utiliser fenêtres temporelles pour calculs de série temporelle; vérifier handling des mois sans ventes.
{{ config(
    materialized='view'
) }}

-- Sales trend analysis - Analyse des tendances de ventes
with sales_trends as (
    select
        toYear(o.order_date) as sales_year,
        toMonth(o.order_date) as sales_month,
        toQuarter(o.order_date) as sales_quarter,
        formatDateTime(o.order_date, '%Y-%m') as year_month,
        s.store_name,
        s.city,
        s.state,
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as unique_customers,
        sum(oi.quantity) as total_items_sold,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as total_revenue,
        sum(oi.quantity * oi.discount * oi.list_price) as total_discounts_given,
        avg(oi.quantity * oi.list_price * (1 - oi.discount)) as avg_order_value,
        -- Growth metrics (compared to previous month)
        lag(sum(oi.quantity * oi.list_price * (1 - oi.discount))) over (
            partition by s.store_id
            order by toYear(o.order_date), toMonth(o.order_date)
        ) as prev_month_revenue
        -- Removed complex category aggregation due to ClickHouse limitations
    from {{ ref('stg_bikelocal__orders') }} o
    left join {{ ref('stg_bike_shop__stores') }} s on o.store_id = s.store_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on o.order_id = oi.order_id
    left join {{ ref('stg_bike_shop__products') }} p on oi.product_id = p.product_id
    left join {{ ref('stg_bike_shop__categories') }} c on p.category_id = c.category_id
    group by
        toYear(o.order_date),
        toMonth(o.order_date),
        toQuarter(o.order_date),
        formatDateTime(o.order_date, '%Y-%m'),
        s.store_id,
        s.store_name,
        s.city,
        s.state
)

select
    sales_year,
    sales_month,
    sales_quarter,
    year_month,
    store_name,
    city,
    state,
    total_orders,
    unique_customers,
    total_items_sold,
    total_revenue,
    total_discounts_given,
    avg_order_value,
    prev_month_revenue,
    (total_revenue - prev_month_revenue) / nullif(prev_month_revenue, 0) * 100 as revenue_growth_pct
from sales_trends
order by sales_year desc, sales_month desc, store_name