{{ config(
    materialized='table',
    
) }}

WITH inventory_status AS (
    SELECT
        -- Dimensions magasin depuis intermediate
        ips.store_name,
        ips.city,
        ips.state,
        ips.region,

        -- Dimensions produit depuis intermediate
        ips.product_name,
        ips.brand_name,
        ips.category_name,
        ips.price_tier,

        -- Métriques de stock depuis intermediate
        ips.total_stock_quantity as current_stock,
        (ips.total_stock_quantity * ips.list_price) as stock_value,
        CASE
            WHEN ips.total_stock_quantity = 0 THEN 'Out of Stock'
            WHEN ips.total_stock_quantity <= 5 THEN 'Critical'
            WHEN ips.total_stock_quantity <= 15 THEN 'Low'
            WHEN ips.total_stock_quantity <= 50 THEN 'Normal'
            ELSE 'High'
        END as stock_status,

        -- Métriques d'optimisation depuis intermediate
        iso.monthly_sales_velocity,
        iso.months_of_stock_coverage,
        iso.stock_optimization_status,
        iso.revenue_impact,
        iso.recommendation,

        -- Classifications business
        CASE
            WHEN ips.total_stock_quantity = 0 OR ips.total_stock_quantity <= 5 THEN 'Action Required'
            WHEN iso.stock_optimization_status = 'Overstocked' THEN 'Review Stock'
            WHEN iso.stock_optimization_status = 'Understocked' THEN 'Restock Needed'
            ELSE 'Optimal'
        END as inventory_action_priority,

        -- Valeur business
        CASE
            WHEN (ips.total_stock_quantity * ips.list_price) >= 50000 THEN 'High Value'
            WHEN (ips.total_stock_quantity * ips.list_price) >= 10000 THEN 'Medium Value'
            ELSE 'Low Value'
        END as stock_value_tier,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('int_inventory__product_stock') }} ips
    LEFT JOIN {{ ref('int_inventory__stock_optimization') }} iso
        ON ips.store_id = iso.store_id AND ips.product_id = iso.product_id
)

SELECT
    store_name,
    city,
    state,
    region,
    product_name,
    brand_name,
    category_name,
    price_tier,
    current_stock,
    stock_value,
    stock_status,
    monthly_sales_velocity,
    months_of_stock_coverage,
    stock_optimization_status,
    revenue_impact,
    recommendation,
    inventory_action_priority,
    stock_value_tier,
    created_at,
    created_by
FROM inventory_status
ORDER BY stock_value DESC, store_name, product_name
