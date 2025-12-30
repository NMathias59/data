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
    st.product_name,
    st.brand_name,
    st.category_name,
    st.current_stock as current_stock,
    st.list_price,

    -- Métriques calculées
    (st.current_stock * st.list_price) as stock_value,
    CASE
        WHEN st.current_stock = 0 THEN 'Out of Stock'
        WHEN st.current_stock <= 5 THEN 'Critical'
        WHEN st.current_stock <= 15 THEN 'Low'
        ELSE 'Normal'
    END as stock_status,

    -- Métriques d'optimisation (valeurs par défaut ou disponibles dans l'int)
    0 as monthly_sales_velocity,
    0 as months_of_stock_coverage,
    'Unknown' as stock_optimization_status,
    'Unknown' as revenue_impact,
    'No recommendation' as recommendation,

    -- Classifications d'urgence
    CASE
        WHEN st.current_stock = 0 THEN 'Immediate Action Required'
        WHEN st.current_stock <= 5 THEN 'Urgent Restock'
        WHEN st.current_stock <= 15 THEN 'Restock Soon'
        ELSE 'Monitor'
    END as action_priority,

    CASE
        WHEN (st.current_stock * st.list_price) >= 10000 THEN 'High Value Stock'
        WHEN (st.current_stock * st.list_price) >= 5000 THEN 'Medium Value Stock'
        WHEN (st.current_stock * st.list_price) >= 1000 THEN 'Low Value Stock'
        ELSE 'Very Low Value Stock'
    END as stock_value_category,

    -- Métriques temporelles
    CASE
        WHEN coalesce(st.current_stock, 0) >= 20 THEN 'Fast Moving'
        WHEN coalesce(st.current_stock, 0) >= 10 THEN 'Medium Moving'
        WHEN coalesce(st.current_stock, 0) >= 5 THEN 'Slow Moving'
        ELSE 'Very Slow Moving'
    END as product_velocity_category,

    -- Métadonnées
    now() as created_at,
    'dbt' as created_by

FROM {{ ref('int_inventory__low_stock_alerts') }} st
    -- Use product/store info available in the intermediate model to avoid extra joins
    -- product metadata columns are already present in `int_inventory__low_stock_alerts` (product_name, brand_name, category_name)
    -- store_name may be present; we still keep a join to `stg_bike_shop__stores` only if we need additional store-level fields
LEFT JOIN {{ ref('stg_bike_shop__stores') }} s ON st.store_id = s.store_id