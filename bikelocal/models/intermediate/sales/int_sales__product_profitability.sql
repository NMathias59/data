-- KPI: Indicateurs de profitabilité par produit
-- Description: Agrège ventes, coûts estimés et marges par produit pour permettre l'analyse de rentabilité.
-- Utilité: Alimente `fct_product_profitability` pour calculs de marge et catégorisation
-- Colonnes clés retournées: product_id, total_units_sold, gross_revenue, estimated_cost_price, estimated_margin, profit_margin_percentage
-- Notes: Coûts estimés calculés par règle métier (ex: 60% du prix) — à valider en production.
{{ config(
    materialized='view'
) }}

-- Product profitability analysis - Analyse de la rentabilité par produit
with product_profitability as (
    select
        p.product_id,
        p.product_name,
        b.brand_name,
        c.category_name,
        p.model_year,
        p.list_price as selling_price,
        -- Assuming a cost structure (this would come from additional data in real scenario)
        -- For demo purposes, we'll use estimated costs
        p.list_price * 0.6 as estimated_cost_price, -- Estimated cost at 60% of selling price
        p.list_price * 0.4 as estimated_margin, -- Estimated margin
        sum(oi.quantity) as total_units_sold,
        sum(oi.quantity * oi.list_price) as gross_revenue,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as net_revenue,
        sum(oi.quantity * oi.discount * oi.list_price) as total_discounts,
        -- Profitability metrics
        sum(oi.quantity * (p.list_price * 0.4 - oi.discount * oi.list_price)) as estimated_profit,
        (sum(oi.quantity * (p.list_price * 0.4 - oi.discount * oi.list_price)) / nullif(sum(oi.quantity * oi.list_price), 0)) * 100 as profit_margin_percentage,
        -- Performance indicators
        count(distinct o.order_id) as orders_containing_product,
        avg(oi.discount) as avg_discount_rate,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) / nullif(count(distinct o.order_id), 0) as avg_revenue_per_order
    from {{ ref('stg_bike_shop__products') }} p
    left join {{ ref('stg_bike_shop__brands') }} b on p.brand_id = b.brand_id
    left join {{ ref('stg_bike_shop__categories') }} c on p.category_id = c.category_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on p.product_id = oi.product_id
    left join {{ ref('stg_bikelocal__orders') }} o on oi.order_id = o.order_id
    group by p.product_id, p.product_name, b.brand_name, c.category_name, p.model_year, p.list_price
)

select * from product_profitability