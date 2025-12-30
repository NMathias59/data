-- KPI: Profitabilité produit (marge, profit estimé, ventes)
-- Description: Table de faits centrale pour analyser rentabilité par produit (unités vendues, marges estimées, profit par produit)
-- Utilité: Utilisée pour prioriser assortiments, fixer prix et actions de pricing/discount
-- Colonnes clés retournées: product_id, product_name, total_units_sold, gross_revenue, estimated_cost_price, estimated_profit, profit_margin_percentage
-- Notes: Certaines valeurs (coûts estimés) sont des approximations et doivent être validées par la finance.
{{ config(
    materialized='table'
) }}

WITH profitability_facts AS (
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
        -- Stock & optimization fields (may be null if not computed in int)
        0 as total_stock_quantity,
        0 as stores_carrying_product,
        0 as avg_stock_per_store,
        0 as monthly_sales_velocity,
        0 as months_of_stock_coverage,
        'Unknown' as stock_optimization_status,
        'Unknown' as revenue_impact,
        'No recommendation' as recommendation,
        -- Derived categories preserved for compatibility
        CASE
            WHEN estimated_profit >= 40 THEN 'High Margin'
            WHEN estimated_profit >= 30 THEN 'Good Margin'
            WHEN estimated_profit >= 20 THEN 'Medium Margin'
            ELSE 'Low Margin'
        END as margin_category,
        CASE
            WHEN total_units_sold >= 100 THEN 'Best Seller'
            WHEN total_units_sold >= 50 THEN 'Good Seller'
            WHEN total_units_sold >= 10 THEN 'Average Seller'
            ELSE 'Slow Seller'
        END as sales_performance_category,
        now() as created_at,
        'dbt' as created_by
    FROM {{ ref('int_sales__product_profitability') }}
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
    toDate('1900-01-01') as first_sale_date,
    toDate('1900-01-01') as last_sale_date,
    orders_containing_product as total_orders,
    0 as total_stock_quantity,
    0 as stores_carrying_product,
    0 as avg_stock_per_store,
    0 as monthly_sales_velocity,
    0 as months_of_stock_coverage,
    'Unknown' as stock_optimization_status,
    'Unknown' as revenue_impact,
    'No recommendation' as recommendation,
    margin_category,
    sales_performance_category,
    created_at,
    created_by
FROM profitability_facts
ORDER BY estimated_profit DESC, total_units_sold DESC