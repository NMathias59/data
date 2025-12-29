{{ config(
    materialized='table'
) }}

WITH category_facts AS (
    SELECT
        -- Dimensions catégorie
        c.category_id,
        c.category_name,
        0 as products_in_category,  -- Default value

        -- Métriques de vente (valeurs par défaut)
        0 as total_orders,
        0 as total_units_sold,
        0 as gross_revenue,
        0 as net_revenue,
        0 as total_discounts,
        0 as avg_product_price,
        0 as avg_discount_rate,
        0 as avg_order_value,

        -- Métriques de performance (valeurs par défaut)
        0 as revenue_rank,
        0 as revenue_contribution_pct,
        0 as products_per_order_ratio,

        -- Métriques de stock (valeurs par défaut)
        0 as total_category_stock,
        0 as avg_stock_per_product,

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

    FROM {{ ref('stg_bike_shop__categories') }} c
)

SELECT
    category_id,
    category_name,
    products_in_category,
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
    category_importance,
    price_segment,
    cross_sell_potential,
    avg_revenue_per_order,
    avg_units_per_product,
    stock_coverage_months,
    created_at,
    created_by
FROM category_facts
ORDER BY revenue_contribution_pct DESC, category_name