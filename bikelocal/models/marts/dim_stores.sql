{{ config(
    materialized='table',
    
) }}

WITH store_data AS (
    SELECT
        store_id,
        store_name,
        phone,
        email,
        street,
        city,
        state,
        zip_code,
        -- Créer une adresse complète pour les rapports
        concat(street, ', ', city, ', ', state, ' ', zip_code) as full_address,
        -- Région géographique (grouper par état pour l'analyse)
        CASE
            WHEN state IN ('CA', 'WA', 'OR', 'NV', 'AZ') THEN 'West Coast'
            WHEN state IN ('TX', 'FL', 'GA', 'NC', 'SC') THEN 'South'
            WHEN state IN ('NY', 'NJ', 'PA', 'MA', 'CT') THEN 'Northeast'
            WHEN state IN ('IL', 'MI', 'OH', 'WI', 'MN') THEN 'Midwest'
            ELSE 'Other'
        END as region,
        -- Type de magasin (basé sur la taille du nom ou autres critères business)
        CASE
            WHEN store_name LIKE '%Main%' OR store_name LIKE '%Central%' THEN 'Flagship'
            WHEN store_name LIKE '%Branch%' THEN 'Branch'
            ELSE 'Standard'
        END as store_type
    FROM {{ ref('stg_bike_shop__stores') }}
)

SELECT
    store_id,
    store_name,
    phone,
    email,
    street,
    city,
    state,
    zip_code,
    full_address,
    region,
    store_type,
    -- Métadonnées pour audit
    now() as created_at,
    'dbt' as created_by
FROM store_data
ORDER BY store_id
