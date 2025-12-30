-- KPI: Ventes détaillées (niveau transaction)
-- Description: Fait des mesures détaillées par ligne de commande (vente) utiles pour analyses opérationnelles et modèle constellation BI
-- Utilité: Source principale pour metrics transactionnels et agrégations ultérieures (marts et rapports)
-- Colonnes clés retournées: order_id, customer_id, product_id, staff_id, store_id, order_date_key, quantity, gross_amount, discount_amount, net_amount, order_year, order_month
-- Notes: Matérialisée en `table` pour performance analytiques; contient métriques dérivées pour faciliter reporting.
{{ config(
    materialized='incremental'
) }}

WITH sales_facts AS (
    SELECT
        -- Clés dimensionnelles
        oi.order_id,
        o.customer_id,
        oi.product_id,
        o.staff_id,
        o.store_id,
        o.order_date as order_date_key,

        -- Métriques de vente de base
        oi.quantity,
        oi.list_price,
        oi.discount,
        (oi.list_price * oi.quantity) as gross_amount,
        (oi.list_price * oi.discount * oi.quantity) as discount_amount,
        ((oi.list_price * oi.quantity) - (oi.list_price * oi.discount * oi.quantity)) as net_amount,

        -- Métriques temporelles
        toYear(o.order_date) as order_year,
        toMonth(o.order_date) as order_month,
        toDayOfMonth(o.order_date) as order_day,
        formatDateTime(o.order_date, '%Y-%m') as order_year_month,

        -- Métriques avancées (valeurs par défaut)
        0 as estimated_cost_price,
        0 as estimated_margin,
        0 as profit_margin_percentage,
        0 as product_net_revenue,
        0 as product_total_discounts,

        -- Métriques de performance produit (valeurs par défaut)
        0 as product_total_quantity_sold,
        0 as product_total_revenue,
        0 as product_total_orders,
        0 as product_avg_selling_price,

        -- Métriques de performance magasin (valeurs par défaut)
        0 as store_total_orders,
        0 as store_unique_customers,
        0 as store_total_revenue,
        0 as store_avg_order_value,

        -- Métriques de catégorie (valeur par défaut)
        0 as category_revenue_contribution_pct,
        0 as category_avg_price,
        0 as products_per_order_ratio,

        -- Métriques de tendance (valeurs par défaut)
        0 as period_total_orders,
        0 as period_unique_customers,
        0 as period_total_revenue,
        0 as period_total_discounts,
        0 as period_avg_order_value,
        0 as prev_month_revenue,
        0 as revenue_growth_pct,

        -- Statut de la commande
        o.order_status,
        o.required_date,
        o.shipped_date,

        -- Métadonnées temporelles
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('stg_bikelocal__order_items') }} oi
    JOIN {{ ref('stg_bikelocal__orders') }} o ON oi.order_id = o.order_id
)

{% if is_incremental() %}
    , latest AS (SELECT coalesce(max(order_date_key), '1900-01-01') AS max_order_date FROM {{ this }})

    SELECT
        order_id,
        customer_id,
        product_id,
        staff_id,
        store_id,
        order_date_key,
        quantity,
        list_price,
        discount,
        gross_amount,
        discount_amount,
        net_amount,
        order_year,
        order_month,
        order_day,
        order_year_month,
        estimated_cost_price,
        estimated_margin,
        profit_margin_percentage,
        product_net_revenue as net_revenue,
        product_total_discounts as total_discounts,
        product_total_revenue,
        product_total_orders,
        product_avg_selling_price as avg_selling_price,
        store_total_orders,
        store_unique_customers,
        store_total_revenue,
        store_avg_order_value,
        category_revenue_contribution_pct as revenue_contribution_pct,
        category_avg_price,
        products_per_order_ratio,
        period_total_orders,
        period_unique_customers,
        period_total_revenue,
        period_total_discounts,
        period_avg_order_value,
        prev_month_revenue,
        revenue_growth_pct,
        order_status,
        required_date,
        shipped_date,
        created_at,
        created_by
    FROM sales_facts sf
    WHERE sf.order_date_key > (SELECT max_order_date FROM latest)
    ORDER BY order_date_key, order_id
{% else %}
    SELECT
        order_id,
        customer_id,
        product_id,
        staff_id,
        store_id,
        order_date_key,
        quantity,
        list_price,
        discount,
        gross_amount,
        discount_amount,
        net_amount,
        order_year,
        order_month,
        order_day,
        order_year_month,
        estimated_cost_price,
        estimated_margin,
        profit_margin_percentage,
        product_net_revenue as net_revenue,
        product_total_discounts as total_discounts,
        product_total_revenue,
        product_total_orders,
        product_avg_selling_price as avg_selling_price,
        store_total_orders,
        store_unique_customers,
        store_total_revenue,
        store_avg_order_value,
        category_revenue_contribution_pct as revenue_contribution_pct,
        category_avg_price,
        products_per_order_ratio,
        period_total_orders,
        period_unique_customers,
        period_total_revenue,
        period_total_discounts,
        period_avg_order_value,
        prev_month_revenue,
        revenue_growth_pct,
        order_status,
        required_date,
        shipped_date,
        created_at,
        created_by
    FROM sales_facts
    ORDER BY order_date_key, order_id
{% endif %}
