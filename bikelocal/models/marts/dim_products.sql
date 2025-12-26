{{ config(
    materialized='table',
    
) }}

WITH product_data AS (
    SELECT
        p.product_id,
        p.product_name,
        p.brand_name,
        p.category_name,
        p.model_year,
        p.list_price,
        -- Calculs business
        CASE
            WHEN p.list_price >= 5000 THEN 'Premium'
            WHEN p.list_price >= 2000 THEN 'High'
            WHEN p.list_price >= 500 THEN 'Medium'
            ELSE 'Entry'
        END as price_tier,
        -- Catégorie simplifiée pour les rapports
        CASE
            WHEN p.category_name LIKE '%Mountain%' THEN 'Mountain Bikes'
            WHEN p.category_name LIKE '%Road%' THEN 'Road Bikes'
            WHEN p.category_name LIKE '%Electric%' THEN 'Electric Bikes'
            WHEN p.category_name LIKE '%Children%' THEN 'Kids Bikes'
            WHEN p.category_name LIKE '%Comfort%' THEN 'Comfort Bikes'
            ELSE 'Accessories'
        END as product_category_group,
        -- Indicateur de nouveauté (basé sur model_year)
        CASE
            WHEN p.model_year >= 2023 THEN 'New'
            WHEN p.model_year >= 2020 THEN 'Recent'
            ELSE 'Legacy'
        END as product_age_category,
        -- Prix estimé de revient (assumption: 60% du prix de vente)
        p.list_price * 0.6 as estimated_cost_price,
        -- Marge estimée
        p.list_price * 0.4 as estimated_margin
    FROM {{ ref('stg_bike_shop__products') }} p
)

SELECT
    product_id,
    product_name,
    brand_name,
    category_name,
    model_year,
    list_price,
    price_tier,
    product_category_group,
    product_age_category,
    estimated_cost_price,
    estimated_margin,
    -- Métadonnées
    now() as created_at,
    'dbt' as created_by
FROM product_data
ORDER BY product_id
