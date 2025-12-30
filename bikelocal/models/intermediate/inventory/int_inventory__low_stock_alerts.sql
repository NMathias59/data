-- KPI: Alertes de stock faible / rupture
-- Description: Identifie produits en rupture ou en niveau critique par magasin et calcule les métriques nécessaires pour prioriser le réapprovisionnement
-- Utilité: Alimente `fct_stock_alerts` (priorisation, action_priority)
-- Colonnes clés retournées: store_id, product_id, current_stock, list_price, stock_status, action_priority
-- Notes: Logique métier : seuils critiques (0, <=5, <=15), peut être paramétrée par SKU/magasin.
{{ config(
    materialized='incremental',
    unique_key='store_id,product_id'
) }}

-- Low stock alerts - Alertes de stock faible
with low_stock_alerts as (
    select
        st.store_id as store_id,
        s.store_name,
        st.product_id as product_id,
        p.product_name,
        b.brand_name,
        c.category_name,
        st.quantity as current_stock,
        case
            when st.quantity = 0 then 'Out of Stock'
            when st.quantity <= 5 then 'Critical'
            when st.quantity <= 10 then 'Low'
            else 'Normal'
        end as stock_status,
        p.list_price,
        st.quantity * p.list_price as stock_value
    from {{ ref('stg_bike_shop__stocks') }} st
    join {{ ref('stg_bike_shop__stores') }} s on st.store_id = s.store_id
    join {{ ref('stg_bike_shop__products') }} p on st.product_id = p.product_id
    left join {{ ref('stg_bike_shop__brands') }} b on p.brand_id = b.brand_id
    left join {{ ref('stg_bike_shop__categories') }} c on p.category_id = c.category_id
    where st.quantity <= 10  -- Alert threshold
)

select * from low_stock_alerts
order by current_stock asc, stock_status