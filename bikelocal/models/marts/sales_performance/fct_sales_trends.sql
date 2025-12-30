-- KPI: Tendances de ventes agrégées (time-series)
-- Description: Table de faits temporelle calculant revenus, croissance, et métriques de tendances par période/magasin/catégorie
-- Utilité: Sert aux analyses temporelles, alertes de croissance et dashboards executive
-- Colonnes clés retournées: year_month, sales_year, sales_month, total_revenue, prev_month_revenue, revenue_growth_pct, revenue_per_customer
-- Notes: Calculs de rolling et growth doivent être revus pour horizon temporel (12 mois glissants déjà disponibles dans rpt_category_growth_analysis)
{{ config(
    materialized='incremental'
) }}

WITH trends_facts AS (
    SELECT
        -- Dimensions temporelles
        concat(toString(sales_year), '-', LPAD(toString(sales_month), 2, '0')) as year_month_key,
        sales_year,
        sales_month,
        sales_quarter,
        year_month,

        -- Dimensions géographiques
        store_name,
        city,
        state,

        -- Métriques de vente depuis intermediate
        total_orders,
        unique_customers,
        total_items_sold,
        total_revenue,
        total_discounts_given,
        avg_order_value,
        prev_month_revenue,
        revenue_growth_pct,

        -- Métriques calculées avancées
        round(total_revenue / nullif(unique_customers, 0), 2) as revenue_per_customer,
        round(total_orders / nullif(unique_customers, 0), 2) as orders_per_customer,
        round(total_discounts_given / nullif(total_revenue, 0) * 100, 2) as discount_rate_pct,

        -- Classifications de performance
        CASE
            WHEN revenue_growth_pct >= 20 THEN 'Strong Growth'
            WHEN revenue_growth_pct >= 10 THEN 'Good Growth'
            WHEN revenue_growth_pct >= 0 THEN 'Stable'
            WHEN revenue_growth_pct >= -10 THEN 'Declining'
            ELSE 'Strong Decline'
        END as growth_category,

        CASE
            WHEN total_revenue >= 50000 THEN 'High Revenue'
            WHEN total_revenue >= 20000 THEN 'Medium Revenue'
            WHEN total_revenue >= 5000 THEN 'Low Revenue'
            ELSE 'Very Low Revenue'
        END as revenue_category,

        -- Métriques saisonnières
        CASE
            WHEN sales_month IN (12, 1, 2) THEN 'Winter'
            WHEN sales_month IN (3, 4, 5) THEN 'Spring'
            WHEN sales_month IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END as season,

        -- Indicateurs de tendance
        CASE
            WHEN revenue_growth_pct > 0 THEN 1
            ELSE 0
        END as is_growing,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('int_sales__trends_analysis') }}
)

SELECT
    year_month_key,
    sales_year,
    sales_month,
    sales_quarter,
    year_month,
    store_name,
    city,
    state,
    total_orders,
    unique_customers,
    total_items_sold,
    total_revenue,
    total_discounts_given,
    avg_order_value,
    prev_month_revenue,
    revenue_growth_pct,
    revenue_per_customer,
    orders_per_customer,
    discount_rate_pct,
    growth_category,
    revenue_category,
    season,
    is_growing,
    created_at,
    created_by
{% if is_incremental() %}
    WITH latest AS (SELECT coalesce(max(year_month), '1900-01') AS max_year_month FROM {{ this }})
    SELECT
        year_month_key,
        sales_year,
        sales_month,
        sales_quarter,
        year_month,
        store_name,
        city,
        state,
        total_orders,
        unique_customers,
        total_items_sold,
        total_revenue,
        total_discounts_given,
        avg_order_value,
        prev_month_revenue,
        revenue_growth_pct,
        revenue_per_customer,
        orders_per_customer,
        discount_rate_pct,
        growth_category,
        revenue_category,
        season,
        is_growing,
        created_at,
        created_by
    FROM trends_facts t
    WHERE t.year_month > (SELECT max_year_month FROM latest)
    ORDER BY sales_year DESC, sales_month DESC, total_revenue DESC
{% else %}
    FROM trends_facts
    ORDER BY sales_year DESC, sales_month DESC, total_revenue DESC
{% endif %}