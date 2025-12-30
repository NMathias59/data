-- KPI: Revenus et métriques par catégorie de produit (niveau mensuel)
-- Description: Agrège les ventes par catégorie, marque et gamme de prix sur une granularité mensuelle.
-- Utilité: Sert de source pour les analyses de contribution à la croissance, classements et pour alimenter les marts de catégorie.
-- Colonnes clés retournées: year, month, year_month, category_name, brand_name, price_tier, total_orders, unique_customers, total_units_sold, gross_revenue, net_revenue, avg_order_value
-- Notes: Agrégation au niveau mois; veiller à utiliser NULL-safe aggregations et à matérialiser en `view` pour réutilisation.
{{ config(
    materialized='view'
) }}

-- Category revenue analysis - Analyse des revenus par catégorie
with category_revenue as (
    select
        c.category_id,
        c.category_name,
        -- Product count in category
        count(distinct p.product_id) as products_in_category,
        -- Sales metrics
        count(distinct o.order_id) as total_orders,
        sum(oi.quantity) as total_units_sold,
        sum(oi.quantity * oi.list_price) as gross_revenue,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as net_revenue,
        sum(oi.quantity * oi.discount * oi.list_price) as total_discounts,
        -- Average metrics
        avg(oi.list_price) as avg_product_price,
        avg(oi.discount) as avg_discount_rate,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) / nullif(count(distinct o.order_id), 0) as avg_order_value,
        -- Category performance ranking
        dense_rank() over (order by sum(oi.quantity * oi.list_price * (1 - oi.discount)) desc) as revenue_rank,
        -- Category contribution percentage
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) /
        sum(sum(oi.quantity * oi.list_price * (1 - oi.discount))) over () * 100 as revenue_contribution_pct,
        -- Growth potential indicators
        count(distinct p.product_id) / nullif(count(distinct o.order_id), 0) as products_per_order_ratio,
        -- Stock analysis for category
        sum(st.quantity) as total_category_stock,
        avg(st.quantity) as avg_stock_per_product
    from {{ ref('stg_bike_shop__categories') }} c
    left join {{ ref('stg_bike_shop__products') }} p on c.category_id = p.category_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on p.product_id = oi.product_id
    left join {{ ref('stg_bikelocal__orders') }} o on oi.order_id = o.order_id
    left join {{ ref('stg_bike_shop__stocks') }} st on p.product_id = st.product_id
    group by c.category_id, c.category_name
)

select * from category_revenue
order by net_revenue desc