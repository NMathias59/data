{{ config(
    materialized='table',
    
) }}

WITH inventory_facts AS (
    SELECT
        -- Utiliser les données agrégées depuis intermediate
        ips.store_id,
        ips.product_id,
        ips.total_stock_quantity as current_stock,

        -- Informations produit depuis intermediate
        ips.product_name,
        ips.brand_name,
        ips.category_name,
        ips.list_price,

        -- Métriques calculées
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

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('int_inventory__product_stock') }} ips
    LEFT JOIN {{ ref('int_inventory__stock_optimization') }} iso
        ON ips.store_id = iso.store_id AND ips.product_id = iso.product_id
)

SELECT
    store_id,
    product_id,
    current_stock,
    product_name,
    brand_name,
    category_name,
    list_price,
    stock_value,
    stock_status,
    monthly_sales_velocity,
    months_of_stock_coverage,
    stock_optimization_status,
    revenue_impact,
    recommendation,
    created_at,
    created_by
FROM inventory_facts
ORDER BY store_id, product_id
