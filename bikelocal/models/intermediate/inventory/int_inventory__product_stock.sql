-- KPI: Stock par produit (niveau produit x magasin)
-- Description: Agrège les niveaux de stock actuels par produit et magasin, calcule les valeurs de stock et status (Out/Low/Normal)
-- Utilité: Source pour les modèles d'optimisation stock et alertes (fct_stock_alerts)
-- Colonnes clés retournées: store_id, product_id, current_stock, list_price, stock_value, stock_status
-- Notes: Mettre à jour fréquences d'actualisation selon cadence d'inventaire (ex: nightly)
{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

-- Product stock levels - Niveaux de stock par produit
with product_stock as (
    select
        p.product_id as product_id,
        p.product_name as product_name,
        b.brand_name as brand_name,
        c.category_name as category_name,
        p.model_year as model_year,
        p.list_price as list_price,
        sum(st.quantity) as total_stock_quantity,
        count(distinct st.store_id) as stores_carrying_product,
        avg(st.quantity) as avg_stock_per_store,
        min(st.quantity) as min_stock_per_store,
        max(st.quantity) as max_stock_per_store
    from {{ ref('stg_bike_shop__products') }} p
    left join {{ ref('stg_bike_shop__brands') }} b on p.brand_id = b.brand_id
    left join {{ ref('stg_bike_shop__categories') }} c on p.category_id = c.category_id
    left join {{ ref('stg_bike_shop__stocks') }} st on p.product_id = st.product_id
    group by p.product_id, p.product_name, b.brand_name, c.category_name, p.model_year, p.list_price
)

select * from product_stock