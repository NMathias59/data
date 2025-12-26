{{ config(
    materialized='table'
) }}

WITH customer_data AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        phone,
        email,
        city,
        state,
        -- Créer zip_code depuis les données disponibles (on peut l'ajouter plus tard si nécessaire)
        '' as zip_code,
        -- Utiliser la segmentation RFM déjà calculée dans intermediate
        customer_segment as rfm_segment,
        -- Région géographique
        CASE
            WHEN state IN ('CA', 'WA', 'OR', 'NV', 'AZ') THEN 'West Coast'
            WHEN state IN ('TX', 'FL', 'GA', 'NC', 'SC') THEN 'South'
            WHEN state IN ('NY', 'NJ', 'PA', 'MA', 'CT') THEN 'Northeast'
            WHEN state IN ('IL', 'MI', 'OH', 'WI', 'MN') THEN 'Midwest'
            ELSE 'Other'
        END as region,
        -- Métriques depuis intermediate
        total_orders,
        total_amount as lifetime_value,
        first_order_date,
        last_order_date,
        avg_order_value,
        days_since_last_order,
        order_frequency,
        monetary_value
    FROM {{ ref('int_sales__customer_orders') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    full_name,
    phone,
    email,
    city,
    state,
    zip_code,
    rfm_segment,
    region,
    total_orders,
    lifetime_value,
    first_order_date,
    last_order_date,
    avg_order_value,
    days_since_last_order,
    order_frequency,
    monetary_value,
    -- Métadonnées
    now() as created_at,
    'dbt' as created_by
FROM customer_data
ORDER BY customer_id
