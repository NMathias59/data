{{ config(
    materialized='table',
    
) }}

SELECT
    -- Données de stock de base
    st.store_id,
    st.product_id,
    st.quantity as current_stock,

    -- Informations produit
    p.product_name,
    b.brand_name,
    c.category_name,
    p.list_price,

    -- Métriques calculées
    (st.quantity * p.list_price) as stock_value,
    CASE
        WHEN st.quantity = 0 THEN 'Out of Stock'
        WHEN st.quantity <= 5 THEN 'Critical'
        WHEN st.quantity <= 15 THEN 'Low'
        WHEN st.quantity <= 50 THEN 'Normal'
        ELSE 'High'
    END as stock_status,

    -- Métriques d'optimisation (valeurs par défaut pour le moment)
    0 as monthly_sales_velocity,
    0 as months_of_stock_coverage,
    'Unknown' as stock_optimization_status,
    'Unknown' as revenue_impact,
    'No recommendation' as recommendation,

    -- Métadonnées
    now() as created_at,
    'dbt' as created_by

FROM {{ ref('stg_bike_shop__stocks') }} st
LEFT JOIN {{ ref('stg_bike_shop__products') }} p ON st.product_id = p.product_id
LEFT JOIN {{ ref('stg_bike_shop__brands') }} b ON p.brand_id = b.brand_id
LEFT JOIN {{ ref('stg_bike_shop__categories') }} c ON p.category_id = c.category_id
