{{ config(
    materialized='incremental'
) }}

WITH inventory_status AS (
    SELECT
        -- Dimensions magasin
        s.store_name,
        s.city,
        s.state,

        -- Dimensions produit
        p.product_name,
        b.brand_name,
        c.category_name,
        CASE
            WHEN p.list_price >= 5000 THEN 'Premium'
            WHEN p.list_price >= 2000 THEN 'High'
            WHEN p.list_price >= 500 THEN 'Medium'
            ELSE 'Entry'
        END as price_tier,

        -- Métriques de stock
        st.quantity as current_stock,
        (st.quantity * p.list_price) as stock_value,
        CASE
            WHEN st.quantity = 0 THEN 'Out of Stock'
            WHEN st.quantity <= 5 THEN 'Critical'
            WHEN st.quantity <= 15 THEN 'Low'
            WHEN st.quantity <= 50 THEN 'Normal'
            ELSE 'High'
        END as stock_status,

        -- Métriques d'optimisation (valeurs par défaut)
        0 as monthly_sales_velocity,
        0 as months_of_stock_coverage,
        'Unknown' as stock_optimization_status,
        'Unknown' as revenue_impact,
        'No recommendation' as recommendation,

        -- Classifications business
        CASE
            WHEN st.quantity = 0 OR st.quantity <= 5 THEN 'Action Required'
            ELSE 'Optimal'
        END as inventory_action_priority,

        -- Valeur business
        CASE
            WHEN (st.quantity * p.list_price) >= 50000 THEN 'High Value'
            WHEN (st.quantity * p.list_price) >= 10000 THEN 'Medium Value'
            ELSE 'Low Value'
        END as stock_value_tier,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('stg_bike_shop__stocks') }} st
    LEFT JOIN {{ ref('stg_bike_shop__stores') }} s ON st.store_id = s.store_id
    LEFT JOIN {{ ref('stg_bike_shop__products') }} p ON st.product_id = p.product_id
    LEFT JOIN {{ ref('stg_bike_shop__brands') }} b ON p.brand_id = b.brand_id
    LEFT JOIN {{ ref('stg_bike_shop__categories') }} c ON p.category_id = c.category_id
)

{% if is_incremental() %}
    -- Incremental: insert new store/product combos only (updates to existing rows require full-refresh)
    WITH existing AS (SELECT store_name, product_name FROM {{ this }})
    SELECT
        i.store_name,
        i.city,
        i.state,
        i.product_name,
        i.brand_name,
        i.category_name,
        i.price_tier,
        i.current_stock,
        i.stock_value,
        i.stock_status,
        i.monthly_sales_velocity,
        i.months_of_stock_coverage,
        i.stock_optimization_status,
        i.revenue_impact,
        i.recommendation,
        i.inventory_action_priority,
        i.stock_value_tier,
        i.created_at,
        i.created_by
    FROM inventory_status i
    LEFT JOIN existing e ON e.store_name = i.store_name AND e.product_name = i.product_name
    WHERE e.store_name IS NULL
    ORDER BY stock_value DESC, store_name, product_name
{% else %}
    SELECT
        store_name,
        city,
        state,
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
{% endif %}
