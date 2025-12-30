{{ config(
    materialized='incremental'
) }}

WITH inventory_status AS (
    SELECT
        ii.store_name,
        st.city,
        st.state,
        ii.product_name,
        ii.brand_name,
        ii.category_name,
        -- Compute price tier from the intermediate list_price
        CASE
            WHEN ii.list_price >= 5000 THEN 'Premium'
            WHEN ii.list_price >= 2000 THEN 'High'
            WHEN ii.list_price >= 500 THEN 'Medium'
            ELSE 'Entry'
        END as price_tier,
        ii.current_stock,
        ii.stock_value,
        CASE
            WHEN ii.current_stock = 0 THEN 'Out of Stock'
            WHEN ii.current_stock <= 5 THEN 'Critical'
            WHEN ii.current_stock <= 15 THEN 'Low'
            WHEN ii.current_stock <= 50 THEN 'Normal'
            ELSE 'High'
        END as stock_status,
        ii.monthly_sales_velocity,
        ii.months_of_stock_coverage,
        ii.stock_optimization_status,
        ii.revenue_impact,
        ii.recommendation,
        CASE WHEN ii.current_stock = 0 OR ii.current_stock <= 5 THEN 'Action Required' ELSE 'Optimal' END as inventory_action_priority,
        CASE WHEN ii.stock_value >= 50000 THEN 'High Value' WHEN ii.stock_value >= 10000 THEN 'Medium Value' ELSE 'Low Value' END as stock_value_tier,
        now() as created_at,
        'dbt' as created_by
    FROM {{ ref('int_inventory__stock_optimization') }} ii
    LEFT JOIN {{ ref('stg_bike_shop__stores') }} st ON ii.store_id = st.store_id
),
inventory_agg AS (
    SELECT
        grouped.store_name,
        grouped.city,
        grouped.state,
        grouped.product_name,
        grouped.brand_name,
        grouped.category_name,
        grouped.price_tier,
        grouped.current_stock,
        grouped.stock_value,
        CASE
            WHEN grouped.current_stock = 0 THEN 'Out of Stock'
            WHEN grouped.current_stock <= 5 THEN 'Critical'
            WHEN grouped.current_stock <= 15 THEN 'Low'
            WHEN grouped.current_stock <= 50 THEN 'Normal'
            ELSE 'High'
        END as stock_status,
        grouped.monthly_sales_velocity,
        grouped.months_of_stock_coverage,
        grouped.stock_optimization_status,
        grouped.revenue_impact,
        grouped.recommendation,
        CASE WHEN grouped.current_stock = 0 OR grouped.current_stock <= 5 THEN 'Action Required' ELSE 'Optimal' END as inventory_action_priority,
        CASE WHEN grouped.stock_value >= 50000 THEN 'High Value' WHEN grouped.stock_value >= 10000 THEN 'Medium Value' ELSE 'Low Value' END as stock_value_tier,
        now() as created_at,
        'dbt' as created_by
    FROM (
        SELECT
            store_name,
            any(city) as city,
            any(state) as state,
            product_name,
            any(brand_name) as brand_name,
            any(category_name) as category_name,
            any(price_tier) as price_tier,
            sum(current_stock) as current_stock,
            sum(stock_value) as stock_value,
            sum(monthly_sales_velocity) as monthly_sales_velocity,
            max(months_of_stock_coverage) as months_of_stock_coverage,
            any(stock_optimization_status) as stock_optimization_status,
            any(revenue_impact) as revenue_impact,
            any(recommendation) as recommendation
        FROM inventory_status
        GROUP BY store_name, product_name
    ) AS grouped
)

{% if is_incremental() %}
    -- Incremental: insert new store/product combos only (updates to existing rows require full-refresh)
    , existing AS (SELECT store_name, product_name FROM {{ this }})
    SELECT
        i.store_name,
        i.city,
        i.state,
        i.product_name,
        i.brand_name,
        i.category_name,
        i.price_tier,
        i.current_stock,
        i.stock_value,
        i.stock_status,
        i.monthly_sales_velocity,
        i.months_of_stock_coverage,
        i.stock_optimization_status,
        i.revenue_impact,
        i.recommendation,
        i.inventory_action_priority,
        i.stock_value_tier,
        i.created_at,
        i.created_by
    FROM inventory_agg i
    LEFT JOIN existing e ON e.store_name = i.store_name AND e.product_name = i.product_name
    WHERE e.store_name IS NULL
    ORDER BY stock_value DESC, store_name, product_name
{% else %}
    SELECT
        store_name,
        city,
        state,
        product_name,
        brand_name,
        category_name,
        price_tier,
        current_stock,
        stock_value,
        stock_status,
        monthly_sales_velocity,
        months_of_stock_coverage,
        stock_optimization_status,
        revenue_impact,
        recommendation,
        inventory_action_priority,
        stock_value_tier,
        created_at,
        created_by
    FROM inventory_agg
    ORDER BY stock_value DESC, store_name, product_name
{% endif %}
