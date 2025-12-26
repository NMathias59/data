{{ config(
    materialized='table'
) }}

WITH profitability_facts AS (
    SELECT
        -- Dimensions depuis intermediate
        pp.product_id,
        pp.product_name,
        pp.brand_name,
        pp.category_name,
        pp.model_year,

        -- Métriques de prix et coût
        pp.selling_price,
        pp.estimated_cost_price,
        pp.estimated_margin,
        pp.total_units_sold,
        pp.gross_revenue,
        pp.net_revenue,
        pp.total_discounts,
        pp.estimated_profit,
        pp.profit_margin_percentage,
        pp.orders_containing_product,
        pp.avg_discount_rate,
        pp.avg_revenue_per_order,

        -- Métriques de performance produit
        pfp.first_sale_date,
        pfp.last_sale_date,
        pfp.total_orders,

        -- Métriques de stock
        ips.total_stock_quantity,
        ips.stores_carrying_product,
        ips.avg_stock_per_store,

        -- Métriques d'optimisation stock
        iso.monthly_sales_velocity,
        iso.months_of_stock_coverage,
        iso.stock_optimization_status,
        iso.revenue_impact,
        iso.recommendation,

        -- Classifications business
        CASE
            WHEN pp.profit_margin_percentage >= 40 THEN 'High Margin'
            WHEN pp.profit_margin_percentage >= 30 THEN 'Good Margin'
            WHEN pp.profit_margin_percentage >= 20 THEN 'Medium Margin'
            ELSE 'Low Margin'
        END as margin_category,

        CASE
            WHEN pp.total_units_sold >= 100 THEN 'Best Seller'
            WHEN pp.total_units_sold >= 50 THEN 'Good Seller'
            WHEN pp.total_units_sold >= 10 THEN 'Average Seller'
            ELSE 'Slow Seller'
        END as sales_performance_category,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('int_sales__product_profitability') }} pp
    LEFT JOIN {{ ref('int_sales__product_performance') }} pfp ON pp.product_id = pfp.product_id
    LEFT JOIN {{ ref('int_inventory__product_stock') }} ips ON pp.product_id = ips.product_id
    LEFT JOIN {{ ref('int_inventory__stock_optimization') }} iso ON pp.product_id = iso.product_id
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
    total_orders,
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