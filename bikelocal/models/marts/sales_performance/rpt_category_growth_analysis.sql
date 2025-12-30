{{ config(
    materialized='table'
) }}

WITH monthly_category_sales AS (
    -- Ventes mensuelles par catégorie
    SELECT
        toYear(o.order_date) as year,
        toMonth(o.order_date) as month,
        formatDateTime(o.order_date, '%Y-%m') as year_month,
        c.category_name,
        b.brand_name,

        -- Classification par gamme de prix
        CASE
            WHEN p.list_price >= 5000 THEN 'Premium'
            WHEN p.list_price >= 2000 THEN 'High-End'
            WHEN p.list_price >= 1000 THEN 'Mid-Range'
            WHEN p.list_price >= 500 THEN 'Entry-Level'
            ELSE 'Budget'
        END as price_tier,

        -- Métriques de vente
        count(DISTINCT o.order_id) as total_orders,
        count(DISTINCT o.customer_id) as unique_customers,
        sum(oi.quantity) as total_units_sold,
        sum(oi.list_price * oi.quantity) as gross_revenue,
        sum((oi.list_price * oi.quantity) - (oi.list_price * oi.discount * oi.quantity)) as net_revenue,

        -- Métriques calculées
        round(net_revenue / nullif(total_orders, 0), 2) as avg_order_value,
        round(net_revenue / nullif(unique_customers, 0), 2) as avg_customer_value

    FROM {{ ref('stg_bikelocal__orders') }} o
    JOIN {{ ref('stg_bikelocal__order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('stg_bike_shop__products') }} p ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg_bike_shop__brands') }} b ON p.brand_id = b.brand_id
    LEFT JOIN {{ ref('stg_bike_shop__categories') }} c ON p.category_id = c.category_id

    GROUP BY
        toYear(o.order_date),
        toMonth(o.order_date),
        formatDateTime(o.order_date, '%Y-%m'),
        c.category_name,
        b.brand_name,
        CASE
            WHEN p.list_price >= 5000 THEN 'Premium'
            WHEN p.list_price >= 2000 THEN 'High-End'
            WHEN p.list_price >= 1000 THEN 'Mid-Range'
            WHEN p.list_price >= 500 THEN 'Entry-Level'
            ELSE 'Budget'
        END
),

category_growth_analysis AS (
    SELECT
        year_month,
        year,
        month,
        category_name,
        brand_name,
        price_tier,

        -- Métriques actuelles
        total_orders,
        unique_customers,
        total_units_sold,
        gross_revenue,
        net_revenue,
        avg_order_value,
        avg_customer_value,

        -- Cumul à date (depuis début d'année)
        sum(net_revenue) OVER (
            PARTITION BY category_name, price_tier, year
            ORDER BY month
            ROWS UNBOUNDED PRECEDING
        ) as revenue_ytd,

        -- Contribution à la croissance (comparaison mois précédent)
        net_revenue - lag(net_revenue) OVER (
            PARTITION BY category_name, price_tier
            ORDER BY year, month
        ) as revenue_growth_abs,

        round(
            (net_revenue - lag(net_revenue) OVER (
                PARTITION BY category_name, price_tier
                ORDER BY year, month
            )) / nullif(lag(net_revenue) OVER (
                PARTITION BY category_name, price_tier
                ORDER BY year, month
            ), 0) * 100, 2
        ) as revenue_growth_pct,

        -- Métadonnées temporelles
        now() as created_at,
        'dbt' as created_by

    FROM monthly_category_sales
),

category_growth_enriched AS (
    SELECT
        *,
        avg(net_revenue) OVER (
            PARTITION BY category_name, price_tier
            ORDER BY year, month
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as revenue_12m_rolling_avg
    FROM category_growth_analysis
),

category_growth_final AS (
    SELECT
        *,
        lag(revenue_12m_rolling_avg) OVER (
            PARTITION BY category_name, price_tier
            ORDER BY year, month
        ) as revenue_12m_prev,
        round(
            (revenue_12m_rolling_avg - lag(revenue_12m_rolling_avg) OVER (
                PARTITION BY category_name, price_tier
                ORDER BY year, month
            )) / nullif(lag(revenue_12m_rolling_avg) OVER (
                PARTITION BY category_name, price_tier
                ORDER BY year, month
            ), 0) * 100, 2
        ) as growth_contribution_12m_pct
    FROM category_growth_enriched
)

SELECT
    year_month,
    year,
    month,
    category_name,
    brand_name,
    price_tier,
    total_orders,
    unique_customers,
    total_units_sold,
    gross_revenue,
    net_revenue,
    avg_order_value,
    avg_customer_value,
    revenue_12m_rolling_avg,
    revenue_ytd,
    revenue_growth_abs,
    revenue_growth_pct,
    growth_contribution_12m_pct,
    created_at,
    created_by
FROM category_growth_final
ORDER BY category_name, price_tier, year, month