-- KPI: Profil client enrichi (RFM & LTV)
-- Description: Dimension client enrichie avec métriques RFM, LTV et indicateurs de rétention
-- Utilité: Sert comme table de référence pour analyses clients, segmentation et calculs LTV (rpt_customer_ltv)
-- Colonnes clés retournées: customer_id, full_name, rfm_segment, total_orders, lifetime_value, first_order_date, last_order_date, avg_order_value
-- Notes: Les valeurs LTV et RFM peuvent provenir d'intermediate models; garder cohérence des calculs.
{{ config(
    materialized='table'
) }}

WITH customer_data AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        concat(c.first_name, ' ', c.last_name) as full_name,
        c.phone,
        c.email,
        c.city,
        c.state,
        -- Créer zip_code depuis les données disponibles (on peut l'ajouter plus tard si nécessaire)
        '' as zip_code,
        -- Utiliser la segmentation RFM (valeur par défaut)
        'Unknown' as rfm_segment,
        -- Région géographique
        CASE
            WHEN c.state IN ('CA', 'WA', 'OR', 'NV', 'AZ') THEN 'West Coast'
            WHEN c.state IN ('TX', 'FL', 'GA', 'NC', 'SC') THEN 'South'
            WHEN c.state IN ('NY', 'NJ', 'PA', 'MA', 'CT') THEN 'Northeast'
            WHEN c.state IN ('IL', 'MI', 'OH', 'WI', 'MN') THEN 'Midwest'
            ELSE 'Other'
        END as region,
        -- Métriques depuis intermediate (join sur int_sales__customer_orders)
        coalesce((SELECT any(total_orders) FROM {{ ref('int_sales__customer_orders') }} WHERE customer_id = c.customer_id), 0) as total_orders,
        coalesce((SELECT any(total_amount) FROM {{ ref('int_sales__customer_orders') }} WHERE customer_id = c.customer_id), 0) as lifetime_value,
        -- utiliser NULL si pas de commande plutôt qu'une date factice
        (SELECT any(first_order_date) FROM {{ ref('int_sales__customer_orders') }} WHERE customer_id = c.customer_id) as first_order_date,
        (SELECT any(last_order_date) FROM {{ ref('int_sales__customer_orders') }} WHERE customer_id = c.customer_id) as last_order_date,
        coalesce((SELECT any(avg_order_value) FROM {{ ref('int_sales__customer_orders') }} WHERE customer_id = c.customer_id), 0) as avg_order_value,
        (SELECT any(days_since_last_order) FROM {{ ref('int_sales__customer_orders') }} WHERE customer_id = c.customer_id) as days_since_last_order,
        coalesce((SELECT any(order_frequency) FROM {{ ref('int_sales__customer_orders') }} WHERE customer_id = c.customer_id), 0) as order_frequency,
        coalesce((SELECT any(monetary_value) FROM {{ ref('int_sales__customer_orders') }} WHERE customer_id = c.customer_id), 0) as monetary_value
    FROM {{ ref('stg_bike_shop__customers') }} c
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
