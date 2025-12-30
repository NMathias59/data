-- KPI: Alertes et priorisation stock (action priority)
-- Description: Liste des produits nécessitant action (rupture, restock urgent) avec priorisation business et catégories de valeur
-- Utilité: Dashboard opérationnel pour réapprovisionnement et décisions d'achat
-- Colonnes clés retournées: store_id, product_id, current_stock, list_price, stock_value, action_priority, stock_value_category
-- Notes: Priorisation basée sur seuils métier; peut être enrichie par lead time et supplier info.
{{ config(
    materialized='table'
) }}

SELECT
    st.store_id,
    s.store_name,
    st.product_id,
    p.product_name,
    b.brand_name,
    c.category_name,
    st.quantity as current_stock,
    p.list_price,

    -- Métriques calculées
    (st.quantity * p.list_price) as stock_value,
    CASE
        WHEN st.quantity = 0 THEN 'Out of Stock'
        WHEN st.quantity <= 5 THEN 'Critical'
        WHEN st.quantity <= 15 THEN 'Low'
        ELSE 'Normal'
    END as stock_status,

    -- Métriques d'optimisation (valeurs par défaut)
    0 as monthly_sales_velocity,
    0 as months_of_stock_coverage,
    'Unknown' as stock_optimization_status,
    'Unknown' as revenue_impact,
    'No recommendation' as recommendation,

    -- Classifications d'urgence
    CASE
        WHEN st.quantity = 0 THEN 'Immediate Action Required'
        WHEN st.quantity <= 5 THEN 'Urgent Restock'
        WHEN st.quantity <= 15 THEN 'Restock Soon'
        ELSE 'Monitor'
    END as action_priority,

    CASE
        WHEN (st.quantity * p.list_price) >= 10000 THEN 'High Value Stock'
        WHEN (st.quantity * p.list_price) >= 5000 THEN 'Medium Value Stock'
        WHEN (st.quantity * p.list_price) >= 1000 THEN 'Low Value Stock'
        ELSE 'Very Low Value Stock'
    END as stock_value_category,

    -- Métriques temporelles
    CASE
        WHEN coalesce(st.quantity, 0) >= 20 THEN 'Fast Moving'
        WHEN coalesce(st.quantity, 0) >= 10 THEN 'Medium Moving'
        WHEN coalesce(st.quantity, 0) >= 5 THEN 'Slow Moving'
        ELSE 'Very Slow Moving'
    END as product_velocity_category,

    -- Métadonnées
    now() as created_at,
    'dbt' as created_by

FROM {{ ref('stg_bike_shop__stocks') }} st
LEFT JOIN {{ ref('stg_bike_shop__products') }} p ON st.product_id = p.product_id
LEFT JOIN {{ ref('stg_bike_shop__brands') }} b ON p.brand_id = b.brand_id
LEFT JOIN {{ ref('stg_bike_shop__categories') }} c ON p.category_id = c.category_id
LEFT JOIN {{ ref('stg_bike_shop__stores') }} s ON st.store_id = s.store_id