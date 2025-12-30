-- KPI: Performance de catégorie (revenus, part de croissance, ranking)
-- Description: Table de faits agrégée par catégorie avec métriques de ventes, contributions et classements.
-- Utilité: Table de reporting pour suivre la contribution des catégories à la croissance totale et alimenter les tableaux de bord BI.
-- Colonnes clés retournées: category_id, category_name, total_orders, total_units_sold, gross_revenue, net_revenue, revenue_contribution_pct, revenue_rank
-- Notes: Généralement matérialisée en `table` pour performance; utiliser pour calculs de contribution à la croissance.
{{ config(
    materialized='table'
) }}

WITH category_facts AS (
    SELECT
        -- Dimensions catégorie
        category_id,
        category_name,
        products_in_category,

        -- Métriques de vente
        total_orders,
        total_units_sold,
        gross_revenue,
        net_revenue,
        total_discounts,
        avg_product_price,
        avg_discount_rate,
        avg_order_value,

        -- Métriques de performance
        revenue_rank,
        revenue_contribution_pct,
        products_per_order_ratio,

        -- Métriques de stock
        total_category_stock,
        avg_stock_per_product,

        -- Classifications business
        CASE
            WHEN revenue_contribution_pct >= 30 THEN 'Top Category'
            WHEN revenue_contribution_pct >= 15 THEN 'Major Category'
            WHEN revenue_contribution_pct >= 5 THEN 'Medium Category'
            ELSE 'Minor Category'
        END as category_importance,

        CASE
            WHEN avg_product_price >= 2000 THEN 'Premium Category'
            WHEN avg_product_price >= 1000 THEN 'High-End Category'
            WHEN avg_product_price >= 500 THEN 'Mid-Range Category'
            ELSE 'Entry-Level Category'
        END as price_segment,

        CASE
            WHEN products_per_order_ratio >= 2 THEN 'High Cross-Sell'
            WHEN products_per_order_ratio >= 1.5 THEN 'Medium Cross-Sell'
            ELSE 'Low Cross-Sell'
        END as cross_sell_potential,

        -- Métriques calculées
        round(net_revenue / nullif(total_orders, 0), 2) as avg_revenue_per_order,
        round(total_units_sold / nullif(products_in_category, 0), 2) as avg_units_per_product,
        round(total_category_stock / nullif(total_units_sold, 0), 2) as stock_coverage_months,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('int_sales__category_revenue') }}
)
, category_agg AS (
    SELECT
        grouped.category_id,
        grouped.category_name,
        grouped.total_orders,
        grouped.total_units_sold,
        grouped.gross_revenue,
        grouped.net_revenue,
        grouped.total_discounts,
        grouped.avg_product_price,
        grouped.avg_discount_rate,
        grouped.avg_order_value,
        dense_rank() OVER (ORDER BY grouped.net_revenue DESC) as revenue_rank,
        sum(grouped.net_revenue) OVER () as grand_net_revenue,
        (grouped.net_revenue / nullif(sum(grouped.net_revenue) OVER (), 0)) * 100 as revenue_contribution_pct,
        (grouped.total_units_sold / nullif(grouped.total_orders, 0)) as products_per_order_ratio,
        0 as total_category_stock,
        0 as avg_stock_per_product,
        now() as created_at,
        'dbt' as created_by
    FROM (
        SELECT
            category_id,
            category_name,
            sum(total_orders) as total_orders,
            sum(total_units_sold) as total_units_sold,
            sum(gross_revenue) as gross_revenue,
            sum(net_revenue) as net_revenue,
            sum(total_discounts) as total_discounts,
            avg(avg_product_price) as avg_product_price,
            avg(avg_discount_rate) as avg_discount_rate,
            avg(avg_order_value) as avg_order_value
        FROM {{ ref('int_sales__category_revenue') }}
        GROUP BY category_id, category_name
    ) AS grouped
)

SELECT
    category_id,
    category_name,
    0 as products_in_category,
    total_orders,
    total_units_sold,
    gross_revenue,
    net_revenue,
    total_discounts,
    avg_product_price,
    avg_discount_rate,
    avg_order_value,
    revenue_rank,
    revenue_contribution_pct,
    products_per_order_ratio,
    total_category_stock,
    avg_stock_per_product,
    CASE
        WHEN revenue_contribution_pct >= 30 THEN 'Top Category'
        WHEN revenue_contribution_pct >= 15 THEN 'Major Category'
        WHEN revenue_contribution_pct >= 5 THEN 'Medium Category'
        ELSE 'Minor Category'
    END as category_importance,
    CASE
        WHEN avg_product_price >= 2000 THEN 'Premium Category'
        WHEN avg_product_price >= 1000 THEN 'High-End Category'
        WHEN avg_product_price >= 500 THEN 'Mid-Range Category'
        ELSE 'Entry-Level Category'
    END as price_segment,
    CASE
        WHEN products_per_order_ratio >= 2 THEN 'High Cross-Sell'
        WHEN products_per_order_ratio >= 1.5 THEN 'Medium Cross-Sell'
        ELSE 'Low Cross-Sell'
    END as cross_sell_potential,
    round(net_revenue / nullif(total_orders, 0), 2) as avg_revenue_per_order,
    round(total_units_sold / nullif(products_in_category, 0), 2) as avg_units_per_product,
    round(total_category_stock / nullif(total_units_sold, 0), 2) as stock_coverage_months,
    created_at,
    created_by
FROM category_agg
ORDER BY revenue_contribution_pct DESC, category_name