{{ config(
    materialized='table',
    
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

        -- Métriques avancées depuis intermediate
        pp.estimated_cost_price,
        pp.estimated_margin,
        pp.profit_margin_percentage,
        pp.net_revenue,
        pp.total_discounts,

        -- Métriques de performance produit
        pfp.total_quantity_sold,
        pfp.total_revenue as product_total_revenue,
        pfp.total_orders as product_total_orders,
        pfp.avg_selling_price,

        -- Métriques de performance magasin
        sp.total_orders as store_total_orders,
        sp.unique_customers as store_unique_customers,
        sp.total_revenue as store_total_revenue,
        sp.avg_order_value as store_avg_order_value,

        -- Métriques de catégorie
        cr.revenue_contribution_pct,
        cr.avg_product_price as category_avg_price,
        cr.products_per_order_ratio,

        -- Métriques temporelles de tendance
        ts.total_orders as period_total_orders,
        ts.unique_customers as period_unique_customers,
        ts.total_revenue as period_total_revenue,
        ts.total_discounts_given as period_total_discounts,
        ts.avg_order_value as period_avg_order_value,
        ts.prev_month_revenue,
        ts.revenue_growth_pct,

        -- Statut de la commande
        o.order_status,
        o.required_date,
        o.shipped_date,

        -- Métadonnées temporelles
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('stg_bikelocal__order_items') }} oi
    JOIN {{ ref('stg_bikelocal__orders') }} o ON oi.order_id = o.order_id
    LEFT JOIN {{ ref('int_sales__product_profitability') }} pp ON oi.product_id = pp.product_id
    LEFT JOIN {{ ref('int_sales__product_performance') }} pfp ON oi.product_id = pfp.product_id
    LEFT JOIN {{ ref('int_sales__store_performance') }} sp ON o.store_id = sp.store_id
    LEFT JOIN {{ ref('int_sales__category_revenue') }} cr ON oi.product_id IN (
        SELECT product_id FROM {{ ref('stg_bike_shop__products') }}
        WHERE category_name = cr.category_name
    )
    LEFT JOIN {{ ref('int_sales__trends_analysis') }} ts ON
        toYear(o.order_date) = ts.sales_year AND
        toMonth(o.order_date) = ts.sales_month AND
        o.store_id = (
            SELECT store_id FROM {{ ref('stg_bike_shop__stores') }}
            WHERE store_name = ts.store_name
        )
)

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
    net_revenue,
    total_discounts,
    product_total_revenue,
    product_total_orders,
    avg_selling_price,
    store_total_orders,
    store_unique_customers,
    store_total_revenue,
    store_avg_order_value,
    revenue_contribution_pct,
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
