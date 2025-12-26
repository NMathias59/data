{{ config(
    materialized='table'
) }}

WITH stock_alerts_facts AS (
    SELECT
        -- Dimensions
        store_id,
        store_name,
        product_id,
        product_name,
        brand_name,
        category_name,
        current_stock,
        list_price,

        -- Métriques calculées
        (current_stock * list_price) as stock_value,
        CASE
            WHEN current_stock = 0 THEN 'Out of Stock'
            WHEN current_stock <= 5 THEN 'Critical'
            WHEN current_stock <= 15 THEN 'Low'
            ELSE 'Normal'
        END as stock_status,

        -- Métriques d'optimisation depuis intermediate
        iso.monthly_sales_velocity,
        iso.months_of_stock_coverage,
        iso.stock_optimization_status,
        iso.revenue_impact,
        iso.recommendation,

        -- Classifications d'urgence
        CASE
            WHEN current_stock = 0 THEN 'Immediate Action Required'
            WHEN current_stock <= 5 THEN 'Urgent Restock'
            WHEN current_stock <= 15 THEN 'Restock Soon'
            WHEN iso.stock_optimization_status = 'Understocked' THEN 'Optimize Stock'
            ELSE 'Monitor'
        END as action_priority,

        CASE
            WHEN (current_stock * list_price) >= 10000 THEN 'High Value Stock'
            WHEN (current_stock * list_price) >= 5000 THEN 'Medium Value Stock'
            WHEN (current_stock * list_price) >= 1000 THEN 'Low Value Stock'
            ELSE 'Very Low Value Stock'
        END as stock_value_category,

        -- Métriques temporelles
        CASE
            WHEN iso.monthly_sales_velocity >= 20 THEN 'Fast Moving'
            WHEN iso.monthly_sales_velocity >= 10 THEN 'Medium Moving'
            WHEN iso.monthly_sales_velocity >= 5 THEN 'Slow Moving'
            ELSE 'Very Slow Moving'
        END as product_velocity_category,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('int_inventory__low_stock_alerts') }} lsa
    LEFT JOIN {{ ref('int_inventory__stock_optimization') }} iso
        ON lsa.store_id = iso.store_id AND lsa.product_id = iso.product_id
)

SELECT
    store_id,
    store_name,
    product_id,
    product_name,
    brand_name,
    category_name,
    current_stock,
    list_price,
    stock_value,
    stock_status,
    monthly_sales_velocity,
    months_of_stock_coverage,
    stock_optimization_status,
    revenue_impact,
    recommendation,
    action_priority,
    stock_value_category,
    product_velocity_category,
    created_at,
    created_by
FROM stock_alerts_facts
ORDER BY
    CASE action_priority
        WHEN 'Immediate Action Required' THEN 1
        WHEN 'Urgent Restock' THEN 2
        WHEN 'Restock Soon' THEN 3
        WHEN 'Optimize Stock' THEN 4
        ELSE 5
    END,
    stock_value DESC