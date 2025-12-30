-- KPI: Vue d'inventaire opérationnelle (valeurs et statut de stock)
-- Description: Table de faits listant les stocks actuels par produit et magasin avec métriques (stock_value, status, coverage)
-- Utilité: Alimente les rapports d'inventaire et actions d'optimisation
-- Colonnes clés retournées: store_id, product_id, current_stock, product_name, list_price, stock_value, stock_status, months_of_stock_coverage
-- Notes: Matérialisée en `table` pour faciliter les jointures et le rafraîchissement coté BI.
{{ config(
    materialized='table'
) }}

SELECT
    -- Données de stock de base (issu de l'intermédiaire)
    st.store_id,
    st.product_id,
    st.current_stock as current_stock,

    -- Informations produit (déjà présentes dans l'int)
    st.product_name,
    st.brand_name,
    st.category_name,
    st.list_price,

    -- Métriques calculées
    (st.current_stock * st.list_price) as stock_value,
    CASE
        WHEN st.current_stock = 0 THEN 'Out of Stock'
        WHEN st.current_stock <= 5 THEN 'Critical'
        WHEN st.current_stock <= 15 THEN 'Low'
        WHEN st.current_stock <= 50 THEN 'Normal'
        ELSE 'High'
    END as stock_status,

    -- Métriques d'optimisation (issu de l'int quand disponibles)
    st.monthly_sales_velocity as monthly_sales_velocity,
    st.months_of_stock_coverage as months_of_stock_coverage,
    st.stock_optimization_status as stock_optimization_status,
    st.revenue_impact as revenue_impact,
    st.recommendation as recommendation,

    -- Métadonnées
    now() as created_at,
    'dbt' as created_by

FROM {{ ref('int_inventory__stock_optimization') }} st
LEFT JOIN {{ ref('stg_bike_shop__products') }} p ON st.product_id = p.product_id
LEFT JOIN {{ ref('stg_bike_shop__brands') }} b ON p.brand_id = b.brand_id
LEFT JOIN {{ ref('stg_bike_shop__categories') }} c ON p.category_id = c.category_id
