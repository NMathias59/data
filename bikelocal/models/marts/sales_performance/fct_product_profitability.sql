{{ config(
    materialized='table'
) }}

WITH profitability_facts AS (
    SELECT
        p.product_id,
        p.product_name,
        b.brand_name,
        c.category_name,
        p.model_year,

        -- Métriques de prix et coût (valeurs par défaut)
        0 as selling_price,
        0 as estimated_cost_price,
        0 as estimated_margin,
        0 as total_units_sold,
        0 as gross_revenue,
        0 as net_revenue,
        0 as total_discounts,
        0 as estimated_profit,
        0 as profit_margin_percentage,
        0 as orders_containing_product,
        0 as avg_discount_rate,
        0 as avg_revenue_per_order,

        -- Métriques de performance produit (valeurs par défaut)
        '1900-01-01' as first_sale_date,
        '1900-01-01' as last_sale_date,
        0 as pfp_total_orders,

        -- Métriques de stock
        0 as total_stock_quantity,
        0 as stores_carrying_product,
        0 as avg_stock_per_store,

        -- Métriques d'optimisation stock (valeurs par défaut)
        0 as monthly_sales_velocity,
        0 as months_of_stock_coverage,
        'Unknown' as stock_optimization_status,
        'Unknown' as revenue_impact,
        'No recommendation' as recommendation,

        CASE
            WHEN 0 >= 40 THEN 'High Margin'
            WHEN 0 >= 30 THEN 'Good Margin'
            WHEN 0 >= 20 THEN 'Medium Margin'
            ELSE 'Low Margin'
        END as margin_category,

        CASE
            WHEN 0 >= 100 THEN 'Best Seller'
            WHEN 0 >= 50 THEN 'Good Seller'
            WHEN 0 >= 10 THEN 'Average Seller'
            ELSE 'Slow Seller'
        END as sales_performance_category,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('stg_bike_shop__products') }} p
    LEFT JOIN {{ ref('stg_bike_shop__brands') }} b ON p.brand_id = b.brand_id
    LEFT JOIN {{ ref('stg_bike_shop__categories') }} c ON p.category_id = c.category_id
)

SELECT
    product_id,
    product_name,
    brand_name,
    category_name,
    model_year,
    selling_price,
    estimated_cost_price,
    estimated_margin,
    total_units_sold,
    gross_revenue,
    net_revenue,
    total_discounts,
    estimated_profit,
    profit_margin_percentage,
    orders_containing_product,
    avg_discount_rate,
    avg_revenue_per_order,
    first_sale_date,
    last_sale_date,
    pfp_total_orders as total_orders,
    total_stock_quantity,
    stores_carrying_product,
    avg_stock_per_store,
    monthly_sales_velocity,
    months_of_stock_coverage,
    stock_optimization_status,
    revenue_impact,
    recommendation,
    margin_category,
    sales_performance_category,
    created_at,
    created_by
FROM profitability_facts
ORDER BY estimated_profit DESC, total_units_sold DESC