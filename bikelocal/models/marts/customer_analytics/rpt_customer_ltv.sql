-- KPI: Lifetime Value (LTV) et indicateurs de rétention client
-- Description: Rapport consolidé calculant LTV, durée de vie du client, statut de rétention et projections annuelles
-- Utilité: Utilisé pour prioriser actions marketing et segmentations à forte valeur
-- Colonnes clés retournées: customer_id, total_orders, lifetime_value, avg_order_value, first_order_date, last_order_date, customer_lifespan_months, projected_annual_value, customer_value_tier
-- Notes: S'assurer que les dates sont au format Date/DateTime pour utiliser dateDiff correctement.
{{ config(
    materialized='incremental'
) }}

WITH customer_ltv AS (
    SELECT
        -- Informations client
        c.customer_id,
        c.full_name,
        c.city,
        c.state,
        c.region,
        c.rfm_segment,

        -- Métriques de valeur vie client
        c.total_orders,
        c.lifetime_value,
        c.avg_order_value,
        c.order_frequency,

        -- Période d'activité
        c.first_order_date,
        c.last_order_date,
        dateDiff('day', toDate(c.first_order_date), toDate(c.last_order_date)) as customer_lifespan_days,
        dateDiff('month', toDate(c.first_order_date), toDate(c.last_order_date)) as customer_lifespan_months,

        -- Métriques de rétention
        c.days_since_last_order,
        CASE
            WHEN c.days_since_last_order <= 30 THEN 'Active'
            WHEN c.days_since_last_order <= 90 THEN 'Recent'
            WHEN c.days_since_last_order <= 180 THEN 'At Risk'
            ELSE 'Churned'
        END as retention_status,

        -- Projections de valeur
        CASE
            WHEN dateDiff('month', toDate(c.first_order_date), toDate(c.last_order_date)) > 0
            THEN c.lifetime_value / dateDiff('month', toDate(c.first_order_date), toDate(c.last_order_date)) * 12  -- Annualisé
            ELSE c.lifetime_value
        END as projected_annual_value,

        -- Score de valeur (basé sur RFM et LTV)
        CASE
            WHEN c.rfm_segment = 'Champion' AND c.lifetime_value >= 10000 THEN 'VIP'
            WHEN c.rfm_segment IN ('Champion', 'Loyal') AND c.lifetime_value >= 5000 THEN 'High Value'
            WHEN c.rfm_segment IN ('Champion', 'Loyal', 'Potential') AND c.lifetime_value >= 1000 THEN 'Medium Value'
            ELSE 'Low Value'
        END as customer_value_tier,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('dim_customers') }} c
)

{% if is_incremental() %}
SELECT
    customer_id,
    full_name,
    city,
    state,
    region,
    rfm_segment,
    total_orders,
    lifetime_value,
    avg_order_value,
    order_frequency,
    first_order_date,
    last_order_date,
    customer_lifespan_days,
    customer_lifespan_months,
    days_since_last_order,
    retention_status,
    projected_annual_value,
    customer_value_tier,
    created_at,
    created_by
FROM customer_ltv cl
WHERE cl.last_order_date > (
    SELECT coalesce(max(last_order_date), '1900-01-01') FROM {{ this }}
)
   OR cl.customer_id NOT IN (SELECT customer_id FROM {{ this }})
ORDER BY lifetime_value DESC, customer_id
{% else %}
SELECT
    customer_id,
    full_name,
    city,
    state,
    region,
    rfm_segment,
    total_orders,
    lifetime_value,
    avg_order_value,
    order_frequency,
    first_order_date,
    last_order_date,
    customer_lifespan_days,
    customer_lifespan_months,
    days_since_last_order,
    retention_status,
    projected_annual_value,
    customer_value_tier,
    created_at,
    created_by
FROM customer_ltv
ORDER BY lifetime_value DESC, customer_id
{% endif %}
