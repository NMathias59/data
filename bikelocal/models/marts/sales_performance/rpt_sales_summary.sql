{{ config(
    materialized='incremental',
    
) }}

WITH sales_summary AS (
    SELECT
        -- Dimensions temporelles
        toYear(order_date) as year,
        toMonth(order_date) as month,
        formatDateTime(order_date, '%Y-%m') as year_month,
        toQuarter(order_date) as quarter,

        -- Dimensions géographiques
        s.city,
        s.state,

        -- Dimensions produit
        b.brand_name,
        c.category_name,
        CASE
            WHEN p.list_price >= 5000 THEN 'Premium'
            WHEN p.list_price >= 2000 THEN 'High'
            WHEN p.list_price >= 500 THEN 'Medium'
            ELSE 'Entry'
        END as price_tier,

        -- Métriques agrégées
        count(DISTINCT o.order_id) as total_orders,
        count(DISTINCT o.customer_id) as unique_customers,
        sum(oi.quantity) as total_items_sold,
        sum(oi.list_price * oi.quantity) as gross_revenue,
        sum(oi.list_price * oi.discount * oi.quantity) as total_discounts,
        sum((oi.list_price * oi.quantity) - (oi.list_price * oi.discount * oi.quantity)) as net_revenue,

        -- Métriques calculées
        round(net_revenue / nullif(total_orders, 0), 2) as avg_order_value,
        round(net_revenue / nullif(unique_customers, 0), 2) as avg_customer_value,
        round(total_discounts / nullif(gross_revenue, 0) * 100, 2) as discount_rate_pct,

        -- Métadonnées
        now() as created_at,
        'dbt' as created_by

    FROM {{ ref('stg_bikelocal__orders') }} o
    JOIN {{ ref('stg_bikelocal__order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('stg_bike_shop__stores') }} s ON o.store_id = s.store_id
    JOIN {{ ref('stg_bike_shop__products') }} p ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg_bike_shop__brands') }} b ON p.brand_id = b.brand_id
    LEFT JOIN {{ ref('stg_bike_shop__categories') }} c ON p.category_id = c.category_id

    GROUP BY
        toYear(order_date),
        toMonth(order_date),
        formatDateTime(order_date, '%Y-%m'),
        toQuarter(order_date),
        s.city,
        s.state,
        b.brand_name,
        c.category_name,
        CASE
            WHEN p.list_price >= 5000 THEN 'Premium'
            WHEN p.list_price >= 2000 THEN 'High'
            WHEN p.list_price >= 500 THEN 'Medium'
            ELSE 'Entry'
        END
)

{% if is_incremental() %}
    -- Incremental run: append months newer than the latest persisted month
    WITH latest AS (SELECT coalesce(max(year_month), '1900-01') AS max_year_month FROM {{ this }})

    SELECT
        year,
        month,
        year_month,
        quarter,
        city,
        state,
        brand_name,
        category_name,
        price_tier,
        total_orders,
        unique_customers,
        total_items_sold,
        gross_revenue,
        total_discounts,
        net_revenue,
        avg_order_value,
        avg_customer_value,
        discount_rate_pct,
        created_at,
        created_by
    FROM sales_summary s
    WHERE s.year_month > (SELECT max_year_month FROM latest)
    ORDER BY year, month, category_name
{% else %}
    SELECT
        year,
        month,
        year_month,
        quarter,
        city,
        state,
        brand_name,
        category_name,
        price_tier,
        total_orders,
        unique_customers,
        total_items_sold,
        gross_revenue,
        total_discounts,
        net_revenue,
        avg_order_value,
        avg_customer_value,
        discount_rate_pct,
        created_at,
        created_by
    FROM sales_summary
    ORDER BY year, month, category_name
{% endif %}
 
