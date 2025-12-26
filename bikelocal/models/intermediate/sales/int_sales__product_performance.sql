{{ config(
    materialized='view'
) }}

-- Product sales performance - Performance des ventes par produit
with product_sales as (
    select
        p.product_id,
        p.product_name,
        b.brand_name,
        c.category_name,
        p.model_year,
        p.list_price as original_price,
        sum(oi.quantity) as total_quantity_sold,
        sum(oi.quantity * oi.list_price * (1 - oi.discount)) as total_revenue,
        count(distinct o.order_id) as total_orders,
        avg(oi.list_price * (1 - oi.discount)) as avg_selling_price,
        min(o.order_date) as first_sale_date,
        max(o.order_date) as last_sale_date
    from {{ ref('stg_bike_shop__products') }} p
    left join {{ ref('stg_bike_shop__brands') }} b on p.brand_id = b.brand_id
    left join {{ ref('stg_bike_shop__categories') }} c on p.category_id = c.category_id
    left join {{ ref('stg_bikelocal__order_items') }} oi on p.product_id = oi.product_id
    left join {{ ref('stg_bikelocal__orders') }} o on oi.order_id = o.order_id
    group by p.product_id, p.product_name, b.brand_name, c.category_name, p.model_year, p.list_price
)

select * from product_sales